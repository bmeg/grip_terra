
## Install dependencies
```
pip install pyyaml grpcio-tools pyAnVIL
```

## Run table scanner

This only collects info on entities in the annvil-datastorage namespace and stores
the information in the `config.yaml` file.
```
./grip_terra.py scan -n anvil-datastorage
```

## Launch the server
```
./grip_terra.py server
```

## List tables
```
grip er localhost:50053 list
```

## Searchable columns for a table
```
grip er info anvil-datastorage/anvil_nhgri_broad_ibd_daly_turner_wes/participant
```

## rows for a table
```
grip er rows anvil-datastorage/anvil_nhgri_broad_ibd_daly_turner_wes/participant
```

## Get row from a table
```
grip er get anvil-datastorage/anvil_nhgri_broad_ibd_daly_turner_wes/participant C1859_SZMC_128
```

## Search a table for a value
```
grip er query anvil-datastorage/anvil_nhgri_broad_ibd_daly_turner_wes/participant gender Male
```
