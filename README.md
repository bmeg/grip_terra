
# Install dependencies
```
pip install pyyaml grpcio-tools firecloud
```

# Run table scanner

This only collects info on entities in the annvil-datastorage namespace and stores
the information in the `config.yaml` file.
```
./grip_terra.py scan --edge -n anvil-datastorage
```
`-n` - limits scan to anvil-datastorage namespace
`--edge` - scans data to look for reference and determine edges

# Launch the server
```
./grip_terra.py server
```

# Testing server table level access

## List tables
```
grip er localhost:50053 list
```

## Searchable columns for a table
```
grip er localhost:50053 info anvil-datastorage/anvil_nhgri_broad_ibd_daly_turner_wes/participant
```

## rows for a table
```
grip er localhost:50053 rows anvil-datastorage/anvil_nhgri_broad_ibd_daly_turner_wes/participant
```

## Get row from a table
```
grip er localhost:50053 get anvil-datastorage/anvil_nhgri_broad_ibd_daly_turner_wes/participant C1859_SZMC_128
```

## Search a table for a value
```
grip er localhost:50053 query anvil-datastorage/anvil_nhgri_broad_ibd_daly_turner_wes/participant gender Male
```


# Build graph model for GRIP
```
./build_graph_model.py config.yaml > graph_model.yaml
```

## Create config for GRIP
Create `grip-config.yaml` file that tells GRIP which graph model to use, and well
as configure an embedded store for additional graphs.
```
Default: badger

Server:
  RequestLogging:
    Enable: true

Drivers:
  terra-driver:
    Gripper:
      ConfigFile: ./graph_model.yaml
      Graph: terra

  badger:
    Badger: grip.db

```

# Launch GRIP
```
grip server --config grip-config.yaml
```

Check if the graph is now online
```
grip list graphs
```
It should include `anvil-terra` in the output list.

## Load schema
```
./config2schema.py config.yaml > schema.json
grip schema post --json schema.json
```

## Start Grip Query
```
git clone https://github.com/bmeg/grip-query.git
cd grip-query && git checkout facet-view
python -m gripquery
```
