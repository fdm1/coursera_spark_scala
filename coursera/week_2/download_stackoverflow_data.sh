#! /bin/bash

set -eu -o pipefail

cd $(dirname $0)

if [ ! -f stackoverflow/src/main/resources/stackoverflow/stackoverflow.csv ]; then
  mkdir -p stackoverflow/src/main/resources/stackoverflow
  cd stackoverflow/src/main/resources/stackoverflow
  wget http://alaska.epfl.ch/~dockermoocs/bigdata/stackoverflow.csv
fi
