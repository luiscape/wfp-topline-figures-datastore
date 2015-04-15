#!/bin/bash

# Activating the DataStore
source venv/bin/activate
python code/create_datastore.py $HDX_KEY data/temp.csv
python code/create_datastore_stag.py $HDX_KEY data/temp.csv