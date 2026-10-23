# Shared settings for the run_*.sh scripts. Sourced, not executed.
MR_DIR=/workspace/mapreduce
STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)
RADAR_LANGS=$(cd /workspace && python3 -c "from spark.common.config import load; print(','.join(load()['languages']))" 2>/dev/null || true)
RADAR_LANGS=${RADAR_LANGS:-en,hi,ta,ja,pt}
NUM_LANGS=$(awk -F, '{print NF}' <<< "$RADAR_LANGS")
RADAR_JAR="$MR_DIR/lib/radar-mr.jar"
[[ -f "$RADAR_JAR" ]] || bash "$MR_DIR/build_jar.sh"
export HADOOP_CLASSPATH="$RADAR_JAR${HADOOP_CLASSPATH:+:$HADOOP_CLASSPATH}"   # client must see the partitioner class

# run_job <log_file> <streaming args...>: runs a streaming job, prints wall time, keeps the full log
run_job () {
  local log="$1"; shift
  local t0=$(date +%s)
  set +e
  # UTF-8 for every Python task (raw non-UTF-8 bytes pass through unchanged).
  # PYTHONPATH=. lets scripts import common_title.py: -files makes symlinks into different
  # cache folders, and Python looks for imports next to the real file, not the link.
  mapred streaming "$@" -cmdenv PYTHONIOENCODING=utf-8:surrogateescape -cmdenv PYTHONPATH=. 2>&1 | tee "$log"
  local rc=${PIPESTATUS[0]}
  set -e
  echo "WALL_SECONDS=$(( $(date +%s) - t0 ))" | tee -a "$log"
  return $rc
}
