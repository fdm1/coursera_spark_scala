#! /bin/bash
set -eu -o pipefail
cd $(dirname $0)

if [ ! -f wikipedia/src/main/resources/wikipedia/wikipedia.dat ]; then
  mkdir -p wikipedia/src/main/resources/wikipedia
  cd wikipedia/src/main/resources/wikipedia
  wget http://alaska.epfl.ch/~dockermoocs/bigdata/wikipedia.dat
fi
