#!/bin/bash

cd ~
virtualenv venv
source venv/bin/activate
cd tool
pip install -r requirements.txt