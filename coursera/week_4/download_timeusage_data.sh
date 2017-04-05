#! /bin/bash

set -eu -o pipefail

cd $(dirname $0)

if [ ! -f timeusage/src/main/resources/timeusage/atussum.csv ]; then
  mkdir -p timeusage/src/main/resources/timeusage
  cd timeusage/src/main/resources/timeusage
  wget http://alaska.epfl.ch/~dockermoocs/bigdata/atussum.csv
fi
