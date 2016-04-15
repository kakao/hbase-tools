#!/usr/bin/env bash

set -e
set -o pipefail

if [ "$(uname)" == "Darwin" ]; then
  if ! export JAVA_HOME=$(/usr/libexec/java_home -v1.7); then
    echo JDK7 is required
    exit 1
  fi
else
  [ -z $JAVA_HOME ] && (echo "JAVA_HOME is not set."; exit 1)
  JAVA_VER=$($JAVA_HOME/bin/java -version 2>&1 | sed 's/java version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')
  [ "$JAVA_VER" -eq 17 ] || (echo "JDK7 is required."; exit 1)
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
