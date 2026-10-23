#!/usr/bin/env bash
# Compiles the custom Java partitioner into mapreduce/lib/radar-mr.jar (run inside edge).
set -euo pipefail
cd "$(dirname "$0")"
rm -rf build && mkdir -p build lib
javac -nowarn -cp "$(hadoop classpath)" -d build java/LangPartitioner.java
jar cf lib/radar-mr.jar -C build .
rm -rf build
echo "built mapreduce/lib/radar-mr.jar"
