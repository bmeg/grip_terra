#!/usr/bin/env python

import os
import sys
import yaml

with open(sys.argv[1]) as handle:
    config = yaml.load(handle.read(), Loader=yaml.SafeLoader)

graphMap = {"vertices" : {}, "edges" : {}, "sources":{"terra":{"host": "localhost:50053"}}}

for namespace, names in config['ENTITIES'].items():
    for name, entities in names.items():
        for entity, fields in entities.items():
            graphMap['vertices'][ "/".join([namespace, name, entity, ""]) ] = {
                "source" : "terra",
                "label" : entity,
                "collection":  "/".join([namespace, name, entity])
            }

for namespace, names in config['EDGE_TABLES'].items():
    for name, entities in names.items():
        for entity, fields in entities.items():
            for field, dst in fields.items():
                graphMap['edges'][ "/".join([namespace, name, entity, field]) ] = {
                "fromVertex" : "/".join([namespace, name, entity, ""]),
                "toVertex" : "/".join([namespace, name, dst, ""]),
                "label" : field,
                "edgeTable" : {
                    "source" : "terra",
                    "collection":  "/".join([namespace, name, entity, field]),
                    "fromField": "$.from",
                    "toField": "$.to"
                }
            }


print(yaml.dump(graphMap))
