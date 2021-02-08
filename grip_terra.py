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
import gripper_pb2
import gripper_pb2_grpc

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

class TerraServicer(gripper_pb2_grpc.GRIPSourceServicer):
    def __init__(self, terra):
        self.terra = terra

    def GetCollections(self, request, context):
        for namespace, name, entityType in self.terra.list_entities():
            o = gripper_pb2.Collection()
            o.name = "%s/%s/%s" % (namespace, name, entityType)
            yield o

    def GetCollectionInfo(self, request, context):
        namespace, name, type = request.name.split("/")
        e = self.terra.get_entity(namespace, name, type)
        o = gripper_pb2.CollectionInfo()
        o.search_fields.extend( e.attributeNames )
        return o

    def GetIDs(self, request, context):
        namespace, name, etype = request.name.split("/")
        for row in self.terra.get_entity_rows(namespace, name, etype):
            o = gripper_pb2.RowID()
            o.id = row['name']
            yield o

    def GetRows(self, request, context):
        namespace, name, etype = request.name.split("/")
        for row in self.terra.get_entity_rows(namespace, name, etype):
            o = gripper_pb2.Row()
            o.id = row['name']
            json_format.ParseDict(row['attributes'], o.data)
            yield o

    def GetRowsByID(self, request_iterator, context):
        for req in request_iterator:
            namespace, name, etype = req.collection.split("/")
            ent = self.terra.get_entity_row(namespace, name, etype, req.id)
            o = gripper_pb2.Row()
            o.id = req.id
            o.requestID = req.requestID
            json_format.ParseDict(ent, o.data)
            yield o

    def GetRowsByField(self, req, context):
        field = re.sub( r'^\$\.', '', req.field) # should be doing full json path, but this will work for now
        namespace, name, etype = req.collection.split("/")
        for row in self.terra.get_entity_rows(namespace, name, etype):
            if row["attributes"].get(field, None) == req.value:
                o = gripper_pb2.Row()
                o.id = row["name"]
                json_format.ParseDict(row["attributes"], o.data)
                yield o

def server(config):
    terra = TerraClient()
    if 'ENTITIES' in config:
        terra.setup_entities(config['ENTITIES'])

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    gripper_pb2_grpc.add_GRIPSourceServicer_to_server(
      TerraServicer(terra), server)
    port = config.get("PORT",50051)
    server.add_insecure_port('[::]:%s' % port)
    server.start()
    print("Serving: %s" % (port))
    server.wait_for_termination()

def scan(config):
    terra = TerraClient(namespaces=config.get("NAMESPACES", None))
    terra.scan_workspaces()

    out = {}
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
    with open("config.yaml", "w") as handle:
        handle.write(yaml.dump({
            "PORT" : "50051",
            "ENTITIES" : out
        }))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    subparser = parser.add_subparsers()
    scan_parser = subparser.add_parser("scan")
    scan_parser.add_argument("-n", "--namespace", action="append")
    scan_parser.set_defaults(func=scan)

    server_parser = subparser.add_parser("server")
    server_parser.set_defaults(func=server, namespace=None)

    args = parser.parse_args()

    if os.path.exists(args.config):
        with open(args.config) as handle:
            config = yaml.load(handle, Loader=yaml.SafeLoader)
    else:
        config = {"PORT" : 50051}

    if args.namespace:
        config["NAMESPACES"] = args.namespace

    args.func(config)
