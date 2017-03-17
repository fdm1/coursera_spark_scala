#! /bin/bash

set -eu -o pipefail

ROOT_DIR=$(dirname $0)/..

echo "Running 'sbt test' for $TEST_SUITE"

cd $ROOT_DIR/coursera/$TEST_SUITE
sbt test

