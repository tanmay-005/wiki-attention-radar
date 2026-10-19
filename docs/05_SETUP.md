# 05 · Setup and Initialisation

Step-by-step guide to go from an empty laptop to a running cluster with the dev slice loaded. Do the steps in order. Each step ends with a **check**.

---

> **The ready-made files** (Dockerfile, configs, compose file, init scripts, smoke tests) are already in the repo. The code blocks below show what they contain. If a block here and a repo file ever differ, **the repo file is the one to use**.

## 0. Requirements

| Item | Minimum | Recommended |
|---|---|---|
| RAM | 8 GB (run compose profiles separately) | 16 GB |
| Free disk | 15 GB (dev slice) | 60 GB+ (full 21-day run), **all on D:** |
| OS | Windows 10/11 with WSL2, macOS, or Linux | — |
| Software | Docker Desktop, Git, Python 3.11 (for dashboard), VS Code | — |

> **This machine: C: is full, D: is empty.** Everything large goes on **D:**: the repo, Docker's disk image (which holds all HDFS, Cassandra and MongoDB data), the WSL swap file, the Python venv and the pip cache. By default all of these land on C:, so each step below says how to move it.

Folder layout on D: used in this guide:

```
D:\projects\wiki-attention-radar\   ← the repo
D:\DockerData\                      ← Docker Desktop disk image (all container data)
D:\wsl\                             ← WSL swap file
D:\pip-cache\                       ← pip download cache
```

## 1. Docker Desktop

1. **Install Docker Desktop** with the WSL2 backend.
   - Optional, to keep the program itself off C: (a few GB): run the installer from a terminal with an install directory, e.g.
     `"Docker Desktop Installer.exe" install --installation-dir=D:\Docker`
     (check that your installer version supports `--installation-dir`; if not, a normal install is fine, since the program is small compared with the data).
2. **Move Docker's data to D: before pulling or building anything.** Docker Desktop → Settings → Resources → Advanced → **Disk image location** → `D:\DockerData` → Apply & restart.
   This disk image is where every image, container and volume lives (HDFS blocks, Cassandra and MongoDB data, the raw download cache). Leaving it on C: would fill C: within the first full download.
3. **Give Docker enough memory and put the swap file on D:.** Create `C:\Users\<you>\.wslconfig` (a tiny text file, so C: is fine for it):
   ```ini
   [wsl2]
   memory=10GB
   processors=4
   swap=4GB
   swapFile=D:\\wsl\\swap.vhdx
   ```
   Create the folder `D:\wsl` first, then run `wsl --shutdown` and restart Docker Desktop.
   (Without `swapFile`, WSL puts a 4 GB swap file in your Windows temp folder on C:.)
4. macOS/Linux: Docker Desktop → Settings → Resources → Memory ≥ 8 GB.

**Check:**
- `docker run --rm hello-world` prints "Hello from Docker!".
- `docker info` shows the memory you set.
- `D:\DockerData` now contains a `.vhdx` file, and C: free space did not drop.

## 2. Repository (on D:)

```powershell
# PowerShell
mkdir D:\projects
cd D:\projects
git clone https://github.com/tanmay-005/wiki-attention-radar.git
cd wiki-attention-radar
```

Keeping the repo on the Windows filesystem (D:) is fine for this project: the repo holds only code and config, which containers read through a bind mount. All heavy data stays inside Docker volumes on `D:\DockerData`. The `.gitattributes` file below prevents CRLF line endings from breaking shell scripts in the containers.

Keep pip's cache off C: as well (once per machine):

```powershell
setx PIP_CACHE_DIR D:\pip-cache
```

Create the skeleton (if starting fresh):

```bash
mkdir -p docs config docker/hadoop/conf docker/hive/conf scripts mapreduce hive/udf \
         spark/jobs spark/common enrichment dashboard/pages data/validation \
         benchmarks/results tests
```

`.gitattributes` (prevents CRLF problems):

```
* text=auto
*.sh  text eol=lf
*.py  text eol=lf
*.hql text eol=lf
*.cql text eol=lf
```

`.gitignore`:

```
data/raw/
data/tmp/
*.gz
*.csv
!data/validation/*.csv
.env
__pycache__/
.venv/
*.class
*.jar
benchmarks/results/*.log
```

**Check:** `git status` shows a clean tree after the first commit.

## 3. Custom Hadoop image

`docker/hadoop/Dockerfile`:

```dockerfile
FROM eclipse-temurin:11-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.3

RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 python3-pip curl wget procps netcat-openbsd less vim-tiny \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
      | tar -xz -C /opt && ln -s /opt/hadoop-${HADOOP_VERSION} /opt/hadoop
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
      | tar -xz -C /opt && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

COPY requirements-edge.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements-edge.txt

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/spark/bin:$PATH

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
```

`docker/hadoop/requirements-edge.txt`:

```
pyyaml
requests
pandas
pyarrow
pymongo
cassandra-driver
pytest
```

`docker/hadoop/entrypoint.sh`:

```bash
#!/usr/bin/env bash
set -e
ROLE="${1:-edge}"
case "$ROLE" in
  namenode)
    if [ ! -d /data/hdfs/namenode/current ]; then
      hdfs namenode -format -clusterid radar -force -nonInteractive
    fi
    exec hdfs namenode ;;
  datanode)        exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager)     exec yarn nodemanager ;;
  historyserver)   exec mapred historyserver ;;
  edge)            exec sleep infinity ;;
  *)               exec "$@" ;;
esac
```

### Hadoop configuration (`docker/hadoop/conf/`, mounted to `/opt/hadoop/etc/hadoop/` for these four files)

`core-site.xml`

```xml
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property>
  <property><name>hadoop.http.staticuser.user</name><value>root</value></property>
</configuration>
```

`hdfs-site.xml`

```xml
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>/data/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>/data/hdfs/datanode</value></property>
  <property><name>dfs.permissions.enabled</name><value>false</value></property>
  <property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
  <property><name>dfs.webhdfs.enabled</name><value>true</value></property>
</configuration>
```

`yarn-site.xml`

```xml
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>resourcemanager</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>4096</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>4</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>512</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>4096</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
  <property><name>yarn.log.server.url</name><value>http://historyserver:19888/jobhistory/logs</value></property>
</configuration>
```

`mapred-site.xml`

```xml
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
  <property><name>mapreduce.map.memory.mb</name><value>1024</value></property>
  <property><name>mapreduce.reduce.memory.mb</name><value>1024</value></property>
  <property><name>yarn.app.mapreduce.am.resource.mb</name><value>1024</value></property>
  <property><name>mapreduce.jobhistory.address</name><value>historyserver:10020</value></property>
  <property><name>mapreduce.jobhistory.webapp.address</name><value>historyserver:19888</value></property>
</configuration>
```

## 4. Hive configuration

`docker/hive/conf/hive-site.xml`:

```xml
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/radar/warehouse</value></property>
  <property><name>hive.exec.dynamic.partition.mode</name><value>nonstrict</value></property>
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
</configuration>
```

Copy the same `core-site.xml` into `docker/hive/conf/` as well.

## 5. docker-compose.yml

`docker/docker-compose.yml`:

```yaml
name: radar

x-hadoop: &hadoop
  image: radar-hadoop:3.3.6
  build: { context: ./hadoop }
  volumes:
    - ./hadoop/conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
    - ./hadoop/conf/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
    - ./hadoop/conf/yarn-site.xml:/opt/hadoop/etc/hadoop/yarn-site.xml
    - ./hadoop/conf/mapred-site.xml:/opt/hadoop/etc/hadoop/mapred-site.xml
  networks: [radar]
  profiles: [hadoop]

services:
  namenode:
    <<: *hadoop
    hostname: namenode
    command: ["namenode"]
    ports: ["127.0.0.1:9870:9870"]
    volumes:
      - ./hadoop/conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./hadoop/conf/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
      - nn-data:/data/hdfs/namenode

  datanode:
    <<: *hadoop
    hostname: datanode
    command: ["datanode"]
    depends_on: [namenode]
    ports: ["127.0.0.1:9864:9864"]
    volumes:
      - ./hadoop/conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./hadoop/conf/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
      - dn-data:/data/hdfs/datanode

  resourcemanager:
    <<: *hadoop
    hostname: resourcemanager
    command: ["resourcemanager"]
    depends_on: [namenode, datanode]
    ports: ["127.0.0.1:8088:8088"]

  nodemanager:
    <<: *hadoop
    hostname: nodemanager
    command: ["nodemanager"]
    depends_on: [resourcemanager]
    ports: ["127.0.0.1:8042:8042"]

  historyserver:
    <<: *hadoop
    hostname: historyserver
    command: ["historyserver"]
    depends_on: [resourcemanager]
    ports: ["127.0.0.1:19888:19888"]

  edge:
    <<: *hadoop
    hostname: edge
    command: ["edge"]
    depends_on: [namenode]
    ports: ["127.0.0.1:4040:4040"]
    volumes:
      - ./hadoop/conf/core-site.xml:/opt/hadoop/etc/hadoop/core-site.xml
      - ./hadoop/conf/hdfs-site.xml:/opt/hadoop/etc/hadoop/hdfs-site.xml
      - ./hadoop/conf/yarn-site.xml:/opt/hadoop/etc/hadoop/yarn-site.xml
      - ./hadoop/conf/mapred-site.xml:/opt/hadoop/etc/hadoop/mapred-site.xml
      - ..:/workspace
      - raw-cache:/data/raw-cache

  hiveserver2:
    image: apache/hive:4.0.1
    hostname: hiveserver2
    environment:
      SERVICE_NAME: hiveserver2
      HIVE_CUSTOM_CONF_DIR: /hive_custom_conf
    volumes:
      - ./hive/conf:/hive_custom_conf
      - ..:/workspace
    ports: ["127.0.0.1:10000:10000", "127.0.0.1:10002:10002"]
    networks: [radar]
    profiles: [hive]

  cassandra:
    image: cassandra:4.1
    hostname: cassandra
    environment:
      MAX_HEAP_SIZE: 1G
      HEAP_NEWSIZE: 200M
      CASSANDRA_CLUSTER_NAME: radar
    volumes: [cass-data:/var/lib/cassandra, ..:/workspace]
    ports: ["127.0.0.1:9042:9042"]
    networks: [radar]
    profiles: [nosql]

  mongo:
    image: mongo:7.0
    hostname: mongo
    volumes: [mongo-data:/data/db, ..:/workspace]
    ports: ["127.0.0.1:27017:27017"]
    networks: [radar]
    profiles: [nosql]

networks:
  radar: {}

volumes:
  nn-data: {}
  dn-data: {}
  cass-data: {}
  mongo-data: {}
  raw-cache: {}
```

> Image tags are pinned on purpose. Before building, confirm each tag exists on Docker Hub (`apache/hive`, `cassandra`, `mongo`, `eclipse-temurin`).

## 6. Build and start

```bash
cd docker
docker compose --profile hadoop build
docker compose --profile hadoop up -d
docker compose --profile hadoop ps
```

**Checks**

| URL | Expect |
|---|---|
| http://localhost:9870 | NameNode UI, "Live Nodes: 1" |
| http://localhost:8088 | YARN UI, 1 active node |
| http://localhost:19888 | JobHistory UI |

```bash
docker compose exec edge hdfs dfs -mkdir -p /radar/raw/hourly /radar/raw/country_daily /radar/staged /radar/output /radar/warehouse /radar/benchmarks
docker compose exec edge hdfs dfs -ls /radar
docker compose exec edge hdfs dfsadmin -report | head -20
```

### Smoke test: MapReduce on YARN

```bash
docker compose exec edge bash -lc '
  yarn jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 2 10'
```

**Check:** prints "Estimated value of Pi is 3.8…" (any value) and the job appears in the YARN UI.

### Smoke test: Hadoop Streaming with Python

```bash
docker compose exec edge bash -lc '
  echo -e "a\nb\na" > /tmp/t.txt && hdfs dfs -put -f /tmp/t.txt /tmp/t.txt &&
  mapred streaming -input /tmp/t.txt -output /tmp/t_out \
    -mapper "python3 -c \"import sys; [print(l.strip()+chr(9)+\"1\") for l in sys.stdin]\"" \
    -reducer "uniq -c" && hdfs dfs -cat /tmp/t_out/*'
```

(If quoting is painful, use the real mapper files in `mapreduce/` instead.)

**Check:** output shows counts for `a` and `b`.

## 7. Hive

```bash
docker compose --profile hadoop --profile hive up -d
docker compose exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -e "CREATE DATABASE IF NOT EXISTS radar; SHOW DATABASES;"
```

**Check:** `radar` is listed, and `hdfs dfs -ls /radar/warehouse` (from `edge`) shows `radar.db`.

Run HQL files from the mounted repo:

```bash
docker compose exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -f /workspace/hive/01_raw_tables.hql
```

## 8. Cassandra and MongoDB

```bash
docker compose --profile nosql up -d
# Cassandra needs about 60 s to start
docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES"
docker compose exec cassandra cqlsh -f /workspace/scripts/init_cassandra.cql
docker compose exec mongo mongosh --quiet /workspace/scripts/init_mongo.js
```

`scripts/init_cassandra.cql` contains the keyspace and tables from `03_DATA_MODEL.md` §4.
`scripts/init_mongo.js` creates the collections and indexes from §5.

**Check:** `cqlsh -e "DESCRIBE TABLES" -k radar` lists 6 tables; `mongosh --eval "db.getSiblingDB('radar').getCollectionNames()"` lists `events, pages, validation`.

## 9. Configuration file

Create `config/settings.yaml` (full contents in `02_ARCHITECTURE.md` §6). Set `wikimedia.user_agent` to a string that includes your contact email.

## 10. Download and load the dev slice

```bash
docker compose exec edge python3 /workspace/scripts/download_hourly.py --dev     # 48 files
docker compose exec edge python3 /workspace/scripts/download_country.py --dev    # 3 files
docker compose exec edge bash /workspace/scripts/load_hdfs.sh
docker compose exec edge hdfs dfs -du -h /radar/raw
docker compose exec edge bash -lc 'hdfs dfs -cat /radar/raw/hourly/dt=2024-06-03/pageviews-20240603-120000.gz | zcat | head -5'
```

**Check:** 48 hourly files and 3 country files are in HDFS, and the sample lines match the format in `03_DATA_MODEL.md` §1.

## 11. Dashboard environment (host)

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate    macOS/Linux: source .venv/bin/activate
pip install -r dashboard/requirements.txt     # streamlit, plotly, pandas, pymongo, cassandra-driver, pyyaml
streamlit run dashboard/app.py
```

**Check:** http://localhost:8501 opens. With empty databases it shows the "no data" state, not an error.

## 12. Daily start / stop

```bash
# start what you need
docker compose --profile hadoop up -d          # ingestion, MapReduce, Spark
docker compose --profile hive up -d            # Hive work
docker compose --profile nosql up -d           # loading + dashboard
# stop everything (data volumes are kept)
docker compose --profile hadoop --profile hive --profile nosql down
# full reset (deletes HDFS and DB data)
docker compose --profile hadoop --profile hive --profile nosql down -v
```

## 13. Initialisation checklist

- [ ] Docker disk image moved to `D:\DockerData`; WSL swap and pip cache on D:
- [ ] Docker memory configured
- [ ] Repo cloned, `.gitattributes` and `.gitignore` in place
- [ ] Hadoop image built; NameNode/YARN/JobHistory UIs reachable
- [ ] Pi job and Python streaming smoke tests passed
- [ ] HiveServer2 reachable; `radar` database created on HDFS
- [ ] Cassandra keyspace and tables created; Mongo collections and indexes created
- [ ] `settings.yaml` filled in (User-Agent with contact)
- [ ] Dev slice in HDFS and format verified
- [ ] Dashboard starts on host
