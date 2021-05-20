#!/usr/bin/env python

import os
import re
import sys
import yaml
import json
import argparse
import requests
import logging

import firecloud.api as FAPI

from concurrent import futures

import grpc
from gripper import gripper_pb2, gripper_pb2_grpc

from google.protobuf import json_format

logging.basicConfig(encoding='utf-8', level=logging.INFO)


class Entities:
    def __init__(self, namespace, name, entityType, idName, attributeNames):
        self.namespace = namespace
        self.name = name
        self.entityType = entityType
        self.idName = idName
        self.attributeNames = attributeNames
        self.data = None

    def _cache(self):
        if self.data is None:
            res = FAPI.get_entities(self.namespace, self.name, self.entityType).json()
            self.data = {}
            for r in res:
                self.data[r['name']] = r

    def list_rows(self):
        self._cache()
        for i in self.data.values():
            yield i

    def get_row(self, id):
        self._cache()
        return self.data[id]


class TerraClient:
    def __init__(self, namespaces=None):
        self.namespaces = namespaces
        self.workspace = {}

    def scan_workspaces(self):
        workspaces = []
        for w in FAPI.list_workspaces().json():
            if self.namespaces is None or w['workspace']['namespace'] in self.namespaces:
                workspaces.append( [w['workspace']['namespace'], w['workspace']['name']] )

        self.workspace = {}
        for namespace, name in workspaces:
            if namespace not in self.workspace:
                self.workspace[namespace] = {}
            if name not in self.workspace[namespace]:
                self.workspace[namespace][name] = {}
            logging.info("Scanning %s %s" % (namespace, name))
            res = FAPI.list_entity_types(namespace, name)
            if res.status_code == 200:
                d = res.json()
                for k, v in d.items():
                    #print(k, v)
                    cols = v["attributeNames"]
                    idName = v['idName']
                    self.workspace[namespace][name][k] = Entities(namespace, name, k, idName, cols)

    def list_entities(self):
        for namespace in self.workspace:
            for name in self.workspace[namespace]:
                for entityType in self.workspace[namespace][name]:
                    yield (namespace, name, entityType)

    def get_entity(self, namespace, name, entityType):
        return self.workspace.get(namespace, {}).get(name, {}).get(entityType, None)

    def setup_entities(self, config):
        self.workspace = {}
        for namespace in config:
            if namespace not in self.workspace:
                self.workspace[namespace] = {}
            for name in config[namespace]:
                if name not in self.workspace[namespace]:
                    self.workspace[namespace][name] = {}
                for etype in config[namespace][name]:
                    idName = config[namespace][name][etype]['idName']
                    attributeNames = config[namespace][name][etype]['attributeNames']
                    e = Entities(namespace, name, etype, idName, attributeNames)
                    self.workspace[namespace][name][etype] = e

    def get_entity_rows(self, namespace, name, etype):
        e = self.get_entity(namespace, name, etype)
        if e:
            return e.list_rows()

    def get_entity_row(self, namespace, name, etype, key):
        e = self.get_entity(namespace, name, etype)
        if e:
            return e.get_row(key)

class EdgeTable:
    def __init__(self, namespace, name, etype, field):
        self.namespace = namespace
        self.name = name
        self.etype = etype
        self.field = field
        self.data = None

    def _cache(self):
        res = FAPI.get_entities(self.namespace, self.name, self.etype).json()
        self.data = {}
        for r in res:
            v = r["attributes"][self.field]
            if 'itemsType' in v and v["itemsType"] == "EntityReference":
                for i in v['items']:
                    rname = "%s/%s" % (r['name'], i['entityName'])
                    self.data[rname] = {"from":r['name'], "to":i['entityName']}
            elif 'entityType' in v:
                rname = "%s/%s" % (r['name'], v['entityName'])
                self.data[rname] = {"from":r['name'], "to":v['entityName']}

class EdgeTableClient:
    def __init__(self, terra, edge_config):
        self.terra = terra
        self.tables = {}
        self.edge_config = edge_config

    def list_edge_tables(self):
        for namespace in self.edge_config:
            for name in self.edge_config[namespace]:
                for etype in self.edge_config[namespace][name]:
                    for field in self.edge_config[namespace][name][etype]:
                        yield namespace, name, etype, field

    def _tname(self, namespace, name, etype, field):
        return "%s/%s/%s/%s" % (namespace, name, etype, field)

    def _cache(self, namespace, name, etype, field):
        tname = self._tname(namespace, name, etype, field)
        if tname not in self.tables:
            t = EdgeTable(namespace, name, etype, field)
            t._cache()
            self.tables[tname] = t

    def get_edge_rows(self, namespace, name, etype, field):
        self._cache(namespace, name, etype, field)
        tname = self._tname(namespace, name, etype, field)
        for r in self.tables[tname].data.values():
            yield r

class TerraServicer(gripper_pb2_grpc.GRIPSourceServicer):
    def __init__(self, terra, edge_config):
        self.terra = terra
        self.edges = EdgeTableClient(terra, edge_config)

    def GetCollections(self, request, context):
        for namespace, name, entityType in self.terra.list_entities():
            o = gripper_pb2.Collection()
            o.name = "%s/%s/%s" % (namespace, name, entityType)
            yield o

        for namespace, name, entityType, field in self.edges.list_edge_tables():
            o = gripper_pb2.Collection()
            o.name = "%s/%s/%s/%s" % (namespace, name, entityType, field)
            yield o

    def GetCollectionInfo(self, request, context):
        tmp = request.name.split("/")
        if len(tmp) == 3:
            namespace, name, type = tmp
            e = self.terra.get_entity(namespace, name, type)
            o = gripper_pb2.CollectionInfo()
            o.search_fields.extend( e.attributeNames )
            return o
        if len(tmp) == 4:
            namespace, name, type, field = tmp
            o = gripper_pb2.CollectionInfo()
            o.search_fields.extend( ["$.to", "$.from"] )
            return o

    def GetIDs(self, request, context):
        namespace, name, etype = request.name.split("/")
        for row in self.terra.get_entity_rows(namespace, name, etype):
            o = gripper_pb2.RowID()
            o.id = row['name']
            yield o

    def GetRows(self, request, context):
        tmp = request.name.split("/")
        if len(tmp) == 3:
            namespace, name, etype = tmp
            for row in self.terra.get_entity_rows(namespace, name, etype):
                o = gripper_pb2.Row()
                o.id = row['name']
                json_format.ParseDict(row['attributes'], o.data)
                yield o
        if len(tmp) == 4:
            namespace, name, etype, field = tmp
            for row in self.edges.get_edge_rows(namespace, name, etype, field):
                o = gripper_pb2.Row()
                o.id = "%s/%s" % (row['from'], row["to"])
                json_format.ParseDict(row, o.data)
                yield o

    def GetRowsByID(self, request_iterator, context):
        for req in request_iterator:
            tmp = req.collection.split("/")
            if len(tmp) == 3:
                namespace, name, etype = tmp
                ent = self.terra.get_entity_row(namespace, name, etype, req.id)
                o = gripper_pb2.Row()
                o.id = req.id
                o.requestID = req.requestID
                json_format.ParseDict(ent['attributes'], o.data)
                yield o
            elif len(tmp) == 4:
                namespace, name, etype, field = tmp
                src, dst = req.id.split("/")
                o = gripper_pb2.Row()
                o.id = req.id
                json_format.ParseDict({"to":dst,"from":dst}, o.data)
                yield o

    def GetRowsByField(self, req, context):
        qField = re.sub( r'^\$\.', '', req.field) # should be doing full json path, but this will work for now
        tmp = req.collection.split("/")
        if len(tmp) == 3:
            namespace, name, etype = tmp
            for row in self.terra.get_entity_rows(namespace, name, etype):
                if row["attributes"].get(qField, None) == req.value:
                    o = gripper_pb2.Row()
                    o.id = row["name"]
                    json_format.ParseDict(row["attributes"], o.data)
                    yield o
        elif len(tmp) == 4:
            namespace, name, etype, field = tmp
            for row in self.edges.get_edge_rows(namespace, name, etype, field):
                if row.get(qField, None) == req.value:
                    o = gripper_pb2.Row()
                    o.id = "%s/%s" % (row['from'], row["to"])
                    json_format.ParseDict(row, o.data)
                    yield o


def server(config, args):
    terra = TerraClient()
    if 'ENTITIES' in config:
        terra.setup_entities(config['ENTITIES'])

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    gripper_pb2_grpc.add_GRIPSourceServicer_to_server(
      TerraServicer(terra, config.get("EDGE_TABLES", {})), server)
    port = config.get("PORT",50051)
    server.add_insecure_port('[::]:%s' % port)
    server.start()
    print("Serving: %s" % (port))
    server.wait_for_termination()

def scan(config, args):
    terra = TerraClient(namespaces=config.get("NAMESPACES", None))
    terra.scan_workspaces()

    out = {}
    edges = {}
    for namespace, name, etype in terra.list_entities():
        if namespace not in out:
            out[namespace] = {}
        if name not in out[namespace]:
            out[namespace][name] = {}
        eInfo = terra.get_entity(namespace, name, etype)
        out[namespace][name][etype] = {
            "attributeNames": eInfo.attributeNames,
            "idName": eInfo.idName
        }
        if args.edge:
            ecols = {}
            for row in terra.get_entity_rows(namespace, name, etype):
                for k, v in row['attributes'].items():
                    if isinstance(v, dict):
                        if 'itemsType' in v and v["itemsType"] == "EntityReference":
                            for i in v['items']:
                                ecols[k] = i['entityType']
                        if 'entityType' in v:
                            ecols[k] = v['entityType']
            for field, dst in ecols.items():
                if namespace not in edges:
                    edges[namespace] = {}
                if name not in edges[namespace]:
                    edges[namespace][name] = {}
                if etype not in edges[namespace][name]:
                    edges[namespace][name][etype] = {}
                edges[namespace][name][etype][field] = dst

    with open("config.yaml", "w") as handle:
        handle.write(yaml.dump({
            "PORT" : "50051",
            "EDGE_TABLES": edges,
            "ENTITIES" : out
        }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    subparser = parser.add_subparsers()
    scan_parser = subparser.add_parser("scan")
    scan_parser.add_argument("-n", "--namespace", action="append")
    scan_parser.add_argument("--edge", action="store_true", default=False)
    scan_parser.set_defaults(func=scan)

    server_parser = subparser.add_parser("server")
    server_parser.set_defaults(func=server, namespace=None)

    args = parser.parse_args()

    if os.path.exists(args.config):
        with open(args.config) as handle:
            config = yaml.load(handle, Loader=yaml.SafeLoader)
    else:
        config = {"PORT" : 50053}

    if args.namespace:
        config["NAMESPACES"] = args.namespace

    args.func(config, args)
