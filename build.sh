#!/bin/bash

rm -rf dist build win32/winda.zip
python setup.py py2exe
rm -rf build
7z a win32/winda.zip dist/*

