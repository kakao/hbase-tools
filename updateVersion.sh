#!/usr/bin/env bash
if [ $# -ne 1 ]; then
  echo new version is required
  exit 1
fi
sed -i '' "s/\(.*<version.unified>\).*\(<\/version.unified>\)/\1$1\2/g" hbase0.94/pom.xml hbase0.96/pom.xml hbase0.98/pom.xml hbase1.0/pom.xml
