#!/usr/bin/env bash
# Step 1.5: runs the HDFS commands for the report and saves their output.
# Usage (inside edge): bash /workspace/scripts/hdfs_demo.sh 2024-06-03
set -uo pipefail
DAY="${1:-2024-06-03}"
OUT=/workspace/report/assets/hdfs_demo_${DAY}.txt
DIR=/radar/raw/hourly/dt=$DAY
FILE=$(hdfs dfs -ls -C "$DIR" | head -1)

{
  echo "### HDFS demo for $DIR   ($(date -u '+%Y-%m-%d %H:%M UTC'))"
  echo; echo '$ hdfs dfsadmin -report  (summary)'
  hdfs dfsadmin -report | head -20
  echo; echo "\$ hdfs dfs -count -h $DIR        # DIR_COUNT FILE_COUNT CONTENT_SIZE PATH"
  hdfs dfs -count -h "$DIR"
  echo; echo '$ hdfs dfs -du -s -h /radar/raw/*'
  hdfs dfs -du -s -h /radar/raw/*
  echo; echo "\$ hdfs dfs -ls -h $DIR | head"
  hdfs dfs -ls -h "$DIR" | head -8
  echo; echo "\$ hdfs dfs -stat 'name=%n size=%b blocksize=%o replication=%r' $FILE"
  hdfs dfs -stat 'name=%n size=%b blocksize=%o replication=%r' "$FILE"
  echo; echo "\$ hdfs fsck $FILE -files -blocks -locations"
  hdfs fsck "$FILE" -files -blocks -locations 2>/dev/null | grep -v '^Connecting'
  echo; echo "\$ hdfs dfs -setrep 2 $FILE   (only 1 DataNode, so it stays under-replicated)"
  hdfs dfs -setrep 2 "$FILE"
  hdfs fsck "$FILE" 2>/dev/null | grep -Ei 'under-replicated|Status'
  echo; echo "\$ hdfs dfs -setrep 1 $FILE   (back to 1)"
  hdfs dfs -setrep 1 "$FILE"
  echo; echo '$ hdfs dfs -cat <file> | zcat | head -5'
  hdfs dfs -cat "$FILE" | zcat | head -5
} 2>&1 | tee "$OUT"

echo; echo "Saved to report/assets/hdfs_demo_${DAY}.txt"
