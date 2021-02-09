#!/usr/bin/env python


import sys
import yaml

with open(sys.argv[1]) as handle:
    config = yaml.load(handle, Loader=yaml.SafeLoader)

ents = config['ENTITIES']

vertices = {}
for namespace in ents:
    for name in ents[namespace]:
        for entity in ents[namespace][name]:
            table = "%s/%s/%s" % (namespace, name, entity)
            rec = {
                "source": "terra",
                "label": entity,
                "collection": table
            }
            vertices[table+"/"] = rec

etables = config["EDGE_TABLES"]
edges = {}
for namespace in etables:
    for name in etables[namespace]:
        for entity in etables[namespace][name]:
            for field, dest in etables[namespace][name][entity].items():
                table = "%s/%s/%s/%s" % (namespace, name, entity, field)
                rec = {
                    "fromVertex" : "%s/%s/%s/" % (namespace, name, entity),
                    "toVertex" : "%s/%s/%s/" % (namespace, name, dest),
                    "label" : field,
                    "edgeTable" : {
                        "source" : "terra",
                        "collection" : table,
                        "fromField" : "$." + entity,
                        "toField" : "$." + dest
                    }
                }
                edges[table] = rec


graph_map = {
    "sources" : { 
        "terra" : {
            "host": "localhost:50051"    
        }
    },
    "vertices" : vertices,
    "edges" : edges
}

print(yaml.dump(graph_map))