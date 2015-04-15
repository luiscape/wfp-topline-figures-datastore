#!/bin/bash

# Activating the DataStore
source venv/bin/activate
python tool/code/create_datastore.py $HDX_KEY tool/data/temp.csv
python tool/code/create_datastore_stag.py $HDX_KEY tool/data/temp.csv