#!/bin/sh

wget -nd -r -P src/test/resources/images -A jpeg,jpg,bmp,gif,png https://www.ignositech.com
find  src/test/resources/images | tail -n +2 > src/test/resources/images.txt