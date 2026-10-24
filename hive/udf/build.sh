#!/usr/bin/env bash
# Step 3.3: compiles the Hive UDFs into radar-udf.jar and uploads it to HDFS.
# Runs INSIDE edge:  bash hive/udf/build.sh
#
# Needs Hive's jars to compile against. Copy them once, from PowerShell in docker/:
#   docker compose cp hiveserver2:/opt/hive/lib ../hive/udf/hive-lib
# (hive-lib/ is in .gitignore)
#
# Compiled for Java 8 (--release 8) because the Hive image runs on Java 8,
# while edge has Java 11; classes built for 11 would not load in Hive.
set -euo pipefail
cd "$(dirname "$0")"
if ! ls hive-lib/*.jar hive-lib/lib/*.jar >/dev/null 2>&1; then
  echo "hive-lib/ is missing. From PowerShell in docker/ run:"
  echo "  docker compose cp hiveserver2:/opt/hive/lib ../hive/udf/hive-lib"
  exit 1
fi
rm -rf out && mkdir -p out
javac -nowarn --release 8 -encoding UTF-8 \
  -cp "hive-lib/*:hive-lib/lib/*:$(hadoop classpath)" \
  -d out TitleRules.java NormalizeTitle.java IsContentPage.java
jar cf radar-udf.jar -C out .
rm -rf out
hdfs dfs -mkdir -p /radar/lib
hdfs dfs -put -f radar-udf.jar /radar/lib/radar-udf.jar
echo "uploaded hdfs:///radar/lib/radar-udf.jar ($(du -h radar-udf.jar | cut -f1))"
