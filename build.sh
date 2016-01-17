#!/bin/bash

rm -rf dist build win32/winda.zip
python setup.py py2exe
rm -rf build
cd dist
/c/Progra~1/7-zip/7z a ../win32/winda.zip *
