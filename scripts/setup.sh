#!/bin/sh -e

# install venv and set requirements

# upgrade pip
python3 -m pip install --upgrade pip

# install virtualenv
pip3 install virtualenv

# create dataforj virtualenv
virtualenv venv

# activate and install dependencies
source venv/bin/activate

# upgrade local pip
venv/bin/python -m pip install --upgrade pip

pip3 install -r requirements.txt
