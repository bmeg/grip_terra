#!/usr/bin/env python

import os
import sys
import yaml
import json

with open(sys.argv[1]) as handle:
    config = yaml.load(handle.read(), Loader=yaml.SafeLoader)

schema = {
    "graph" : "anvil-terra",
    "vertices" : [],
    "edges" : []
}


vertMerge = {}
for namespace, names in config['ENTITIES'].items():
    for name, entities in names.items():
        for entity, fields in entities.items():
            vertMerge[entity] = list(set(fields['attributeNames'] + vertMerge.get(entity, [])))

edgeMerge = {}
for namespace, names in config['EDGE_TABLES'].items():
    for name, entities in names.items():
        for entity, fields in entities.items():
            for field, dst in fields.items():
                edgeMerge[(entity,field,dst)] = True

for e, a in vertMerge.items():
    fields = dict( (f,"STRING") for f in a )
    schema["vertices"].append({
        "gid" : e,
        "label" : e,
        "data" : fields
    })

for e in edgeMerge:
    schema["edges"].append({
        "from": e[0],
        "to" : e[2],
        "label": e[1]
    })

print(json.dumps(schema, indent=4))
