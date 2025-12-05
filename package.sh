#!/usr/bin/env bash

set -e
set -o pipefail

if ! java -version 2>&1 | grep -q '1\.8'; then
  echo "ERROR: Java 1.8 is required" >&2
  exit 1
fi

LIFECYCLE=${1:-package}

build() {
  (
    cd hbase$1
    mvn clean $LIFECYCLE -DskipTests

    for module in hbase-table-stat hbase-manager hbase-snapshot; do
      cd $module-$1/target
      for jar in *-jar-with-dependencies.jar; do
        cp -fv $jar ../../../release/${jar/-jar-with-dependencies/}
      done
      cd ../..
    done
  )
}

cd $(dirname ${BASH_SOURCE[0]})
mkdir -p release
rm -f release/*.jar
for v in 0.94 0.96 0.98 1.0 1.2; do
  build $v
done
