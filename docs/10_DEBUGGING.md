# 10 · Debugging Guide

Known problems with this stack and how to fix them, organised by component. When something fails, first find **which stage** failed (the runner prints it), then look in that section.

---

## 0. General method

1. **Read the real error.** For YARN jobs the console message is generic. Open the YARN UI (http://localhost:8088) → application → logs, or run:
   ```bash
   yarn logs -applicationId <application_id> | less
   ```
   For a failed **streaming** task, the Python traceback is in the task's `stderr` log.
2. **Reproduce small.** Use the dev slice, one `dt`, or the local pipe test (`09_TESTING_QA.md` §2).
3. **Check the contract.** Is the previous stage's output where it should be, with `_SUCCESS`, in the right format?
4. **Change one thing at a time** and note it in `report/debug_log.md` (useful for the report's "challenges" section).

Useful commands:

```bash
docker compose ps                         # what is running / restarting
docker compose logs -f --tail=100 namenode
docker stats                              # memory per container
docker compose exec edge hdfs dfsadmin -report
docker compose exec edge yarn node -list
docker compose exec edge yarn application -list -appStates ALL
```

---

## 1. Docker and environment

| Symptom | Cause | Fix |
|---|---|---|
| `/bin/bash^M: bad interpreter` or `$'\r': command not found` | Windows CRLF line endings | `.gitattributes` with `eol=lf`; convert existing files: `sed -i 's/\r$//' file.sh` or VS Code → "LF" |
| Containers exit with code 137 | Out of memory (OOM kill) | Raise the WSL/Docker memory; run fewer profiles; lower `yarn.nodemanager.resource.memory-mb`; Cassandra `MAX_HEAP_SIZE=1G` |
| `no space left on device` | Docker disk full | `docker system df`; delete the raw cache after loading into HDFS; `docker builder prune`; enlarge the Docker disk |
| C: drive filling up | Docker disk image, WSL swap, pip cache or a venv still on C: | Docker Desktop → Settings → Resources → Advanced → Disk image location = `D:\DockerData`; `swapFile=D:\\wsl\\swap.vhdx` in `.wslconfig`; `setx PIP_CACHE_DIR D:\pip-cache`; create the venv inside the repo on D: |
| Very slow file access from containers on Windows | Heavy I/O through the bind-mounted repo on D: | Keep heavy data in Docker volumes (HDFS, raw cache), never in the repo folder; the repo should hold only code and config |
| Port already in use | Another service on 9870/8088/9042/27017 | Change the host-side port in `docker-compose.yml` |
| Image build fails downloading the Hadoop/Spark tarball | Network or wrong version in the URL | Check that the URL exists on archive.apache.org; retry |

## 2. HDFS

| Symptom | Cause | Fix |
|---|---|---|
| `Live Nodes: 0` in the NameNode UI | DataNode cannot register | `docker compose logs datanode`; check `fs.defaultFS` is identical everywhere; keep `registration.ip-hostname-check=false` |
| DataNode fails with "Incompatible clusterIDs" | NameNode was re-formatted but the DataNode volume is old | `docker compose down`; `docker volume rm radar_dn-data`; start again (dev data must be reloaded) |
| `SafeModeException: Name node is in safe mode` | Just started, or too few blocks reported | Wait a minute; check `hdfs dfsadmin -safemode get`; for a lab cluster: `hdfs dfsadmin -safemode leave` |
| `Permission denied: user=hive` | HDFS permissions | `dfs.permissions.enabled=false` (lab), or `hdfs dfs -chmod -R 777 /radar` |
| `File ... could only be written to 0 of the 1 minReplication nodes` | DataNode down or its disk full | Check the DataNode logs and `docker system df` |

## 3. YARN and MapReduce (Hadoop Streaming)

| Symptom | Cause | Fix |
|---|---|---|
| `Could not find or load main class org.apache.hadoop.mapreduce.v2.app.MRAppMaster` | `HADOOP_MAPRED_HOME` not passed to containers | The three `*.env` properties in `mapred-site.xml` (`05_SETUP.md`) |
| Job stays in `ACCEPTED` forever | No NodeManager or not enough memory for the ApplicationMaster | `yarn node -list`; lower `yarn.app.mapreduce.am.resource.mb`; raise NodeManager memory |
| `PipeMapRed.waitOutputThreads(): subprocess failed with code 1` | Your Python script crashed | Open the task's stderr log for the traceback; reproduce with the local pipe test |
| Same, code 127 | `python3` not found on the NodeManager | Use the custom image for the nodemanager too; `-mapper "python3 mr1_mapper.py"` |
| `ModuleNotFoundError` for a helper module | Helper not shipped with the job | Add it to `-files` (e.g. `common_title.py`); import from the working directory |
| `yaml` not found inside a task | Library missing on the NodeManager | Library is in `requirements-edge.txt`, installed in the shared image; rebuild |
| Output directory already exists | Hadoop never overwrites | `hdfs dfs -rm -r -f <output>` at the start of each run script |
| Reducer output has duplicate keys | Key fields not set; Hadoop treated only the first field as the key | `-D stream.num.map.output.key.fields=4` |
| Combiner changes results | Combiner not associative, or it changes the key format | The combiner must emit exactly the mapper's key/value format; summing is safe |
| `SnappyCodec` errors | Native library missing | Hadoop 3.3 ships a Java Snappy implementation; if it still fails, use `Lz4Codec` or `DefaultCodec` and report it |
| Only 1 reducer used | `mapreduce.job.reduces` not set | Set it to the number of languages |
| Hour is wrong / empty | Wrong environment variable for the input file name | Hadoop 3 uses `mapreduce_map_input_file` (dots become underscores). Print `os.environ` once to stderr to check |

## 4. Hive

| Symptom | Cause | Fix |
|---|---|---|
| Tables are created but `SELECT` returns 0 rows | Partitions not registered | `MSCK REPAIR TABLE radar.raw_hourly;` (after every load) |
| RegexSerDe columns all NULL | Regex does not match (extra spaces, `\r`) or wrong escaping | Test the regex in Python on real lines; HQL needs a double backslash (`\\S`); declare columns as STRING first |
| `views` NULL with RegexSerDe | RegexSerDe only supports string output in some versions | Declare STRING and `CAST(views AS BIGINT)` in queries |
| CSV table: all values in the first column | Wrong delimiter | Inspect the file; switch between `OpenCSVSerde` and `FIELDS TERMINATED BY '\t'` |
| Hive writes tables to local disk instead of HDFS | `fs.defaultFS` not picked up | Put `fs.defaultFS` in `hive-site.xml` (done); check with `DESCRIBE FORMATTED radar.hourly_views` → Location |
| `ClassNotFoundException` for the UDF | Jar not found or wrong class name | `CREATE FUNCTION ... USING JAR 'hdfs://namenode:8020/radar/lib/radar-udf.jar'`; the class name must include the package |
| UDF compile errors | Missing jars on the classpath | Compile against `/opt/hive/lib/*` copied from the Hive container plus `$(hadoop classpath)` |
| Dynamic partition insert fails | Strict mode or too many partitions | `SET hive.exec.dynamic.partition.mode=nonstrict; SET hive.exec.max.dynamic.partitions=2000;` |
| Metastore lost after container recreate (`Table not found`, `Invalid function`) | Embedded Derby metastore lives inside the container | `docker compose exec hiveserver2 bash /workspace/hive/rebuild_metadata.sh`. All tables are external, so no data is reloaded |
| `UnsupportedClassVersionError` when calling a UDF | Jar built for Java 11 but Hive runs Java 8 | Build with `hive/udf/build.sh` (uses `--release 8`), re-upload, then restart hiveserver2 or re-run `register.hql` |
| `An external table's location should not be located within managed warehouse root directory` | Hive 4 keeps external tables out of the managed warehouse folder | `metastore.warehouse.dir` points to `/radar/hive-managed` (unused); the external tables live under `/radar/warehouse` |
| hiveserver2 exits after `docker compose restart` with `HiveServer2 running as process 8. Stop it first.` | Old PID file survives the restart and the new process gets the same PID | PID file lives on a tmpfs (`HIVESERVER2_PID_DIR=/hs2-pid` in compose), which is emptied on every restart |
| Hive query stuck at `VERTICES: 00/05 ... 0%` for many minutes, hiveserver2 at ~400% CPU and ~3.5 GB | Tez local mode runs every task inside the 3 GB HiveServer2 JVM; grouping tens of millions of keys fills the heap and the JVM spends its time in garbage collection | Ctrl+C, `docker compose restart hiveserver2`. Do many-key work in Spark (`spark/jobs/load_warehouse.py`, `tests/dq/dq_spark.py`); keep Hive queries to few-group aggregations or filter first |
| Beeline cannot connect | HiveServer2 still starting (it takes a while) | Wait; check `docker compose logs hiveserver2`; UI at http://localhost:10002 |

## 5. Spark

| Symptom | Cause | Fix |
|---|---|---|
| `java.lang.OutOfMemoryError: Java heap space` | Too much data on the driver | Increase `--driver-memory 3g`; never `collect()` large DataFrames; prune candidates earlier |
| Job very slow at a join | Shuffle of a big table | `broadcast()` the small side (candidates); filter before joins; set `spark.sql.shuffle.partitions=64` for a laptop (default 200) |
| `percentile_approx` very slow | Too many groups × high accuracy | Candidate pruning; accuracy 1000 is enough |
| Python worker crashed | Python version mismatch or memory | Same Python in the image for driver and executors (`PYSPARK_PYTHON=python3`); reduce partition size |
| `Path does not exist: hdfs://...` | Previous stage missing or wrong path | Contract check (`08_INTEGRATION.md` §4) |
| Reading Hive ORC gives the wrong columns | Partition column `dt` not in the files | Read the table root path (Spark discovers `dt=` partitions) or use `basePath` |
| Timestamps shifted by 5:30 | Session time zone = local | `spark.sql.session.timeZone=UTC` in every job |
| `--packages` download fails | No internet from the container, or the Ivy cache is broken | Retry; or download the connector jar once to `/workspace/jars/` and use `--jars` |
| Spark UI not reachable | Port 4040 already used by another Spark app (uses 4041) | Check the job log for the actual port |

## 6. Wikidata enrichment

| Symptom | Cause | Fix |
|---|---|---|
| HTTP 403 | Missing or generic User-Agent | Set a descriptive UA with contact info in `settings.yaml` |
| HTTP 429 / timeouts | Too many requests | Sleep 1 s between calls; honour `Retry-After`; batch 50 titles; the cache prevents repeats |
| Many titles have no item | Title is a redirect or URL-encoded | Normalise titles; use `redirects=yes`; fall back to the `item_id` from per-country data |
| Too many events in "Other" | Category rules missing common types | Look at the top "Other" `instance_of` QIDs and extend `category_rules.yaml` |
| Wrong state for an event | P131 chain ambiguous (e.g. stadium → city → district → state) | Walk up until a P300 code; add tests for the misclassified case |
| SPARQL timeout | Too many items per query or `P131*` too deep | ≤ 200 items per query; limit depth with explicit hops |

## 7. Spike detection and events (results look wrong)

| Symptom | Likely cause | Fix |
|---|---|---|
| Thousands of spikes on the first detection day | Baseline window includes days that are not loaded (zeros) | Detection must start ≥ 7 days after the hourly start (`detect_start`) |
| Main page / Special pages in the results | Filter not applied | Check `is_content_page` in MR1 and the UDF |
| Same topic appears 5 times in the top 10 | Grouping missed same-QID merge | Check the QID lookup for those titles; same-QID edges first |
| One huge event containing everything | Generic link targets used for edges | Extend the generic-QID exclusion list; cap links per node |
| Top list full of tiny obscure pages | Ranking by ratio only | Use the score (ratio × log volume); raise `min_views` |
| Strange one-hour spikes on random pages | Automated traffic | Suspicious flag; check desktop share; exclude from the precision count only if labelled noise |
| No country spikes at all | Threshold too strict or the country dataset not loaded | Check `country_daily` row counts; lower `min_views` for country |
| Country spikes everywhere on the first days | Not enough baseline days | Country data must start 14 days before detection |

## 8. Cassandra

| Symptom | Cause | Fix |
|---|---|---|
| `NoHostAvailable` | Cassandra still starting (about 60 s) or wrong host | Wait for "Starting listening for CQL clients" in the logs; use `cassandra` inside Docker, `127.0.0.1` on the host |
| `InvalidRequest: ... ALLOW FILTERING` | Query not using the partition key | Query must give the full partition key (`scope`, `day`); redesign instead of ALLOW FILTERING |
| `Cannot set the value of counter column` | INSERT into a counter table | Use `UPDATE ... SET spikes = spikes + 1` |
| Counter values doubled after re-run | Counters are not idempotent | `TRUNCATE spike_counts` before each load |
| Rows vanished | TTL expired (`trending_now` is 48 h by design) | Expected; reload, or use a longer TTL for the demo day |
| Write timeouts during bulk load | Too much load on one small node | Lower Spark connector concurrency: `spark.cassandra.output.concurrent.writes=2`, smaller batch size |
| `cassandra-driver` install fails on Windows | C extensions need a compiler | Set `CASS_DRIVER_NO_EXTENSIONS=1` before `pip install cassandra-driver` (pure-Python install), or run the dashboard in a container |
| Dates off by one | `day` built in local time | Compute every `day` in UTC |

## 9. MongoDB

| Symptom | Cause | Fix |
|---|---|---|
| `ServerSelectionTimeoutError` | Wrong host (`mongo` vs `127.0.0.1`) or container down | Check `RADAR_ENV`; `docker compose ps` |
| Duplicate key errors on re-run | `insert_many` instead of upsert | `ReplaceOne({_id}, doc, upsert=True)` |
| Slow detail lookups | Missing index | `explain("executionStats")`; create the indexes from `init_mongo.js` |
| Dates shown as strings | Stored strings instead of `datetime` | Insert Python `datetime` objects (UTC) |
| `mongoimport` cannot parse the file | Wrong format flag | `--jsonArray` for JSON arrays; `--type csv --headerline` for CSV |

## 10. Dashboard (Streamlit)

| Symptom | Cause | Fix |
|---|---|---|
| Filters reset unexpectedly | Widget keys missing or changing | Give every widget a fixed `key`; keep state in `st.session_state` |
| State dropdown does not switch the mode | Rule applied after the widget renders | Use `on_change` callbacks to update `session_state` before the rerun |
| Slow on every click | No caching | `@st.cache_data(ttl=300)` on data-access functions; `@st.cache_resource` for DB clients |
| Old results after reload | Cache TTL | Add a "Refresh data" button that calls `st.cache_data.clear()` |
| Chart time axis in the wrong timezone | Naive datetimes | Store UTC; convert for display; show IST in the tooltip |

---

## Debug log template (`report/debug_log.md`)

```markdown
### <date> · <stage> · <one-line symptom>
- Error: <exact message>
- Cause: <what was actually wrong>
- Fix: <what changed (file, setting)>
- Time lost: <minutes>
```
