#!/usr/bin/env bash
# Versions + service status -> outputs/logs/env.txt ; compatibility checks -> outputs/logs/compat.md
cd "$(dirname "$0")/.." && source scripts/env.sh 2>/dev/null
mkdir -p outputs/logs
port() { (echo > /dev/tcp/127.0.0.1/$1) 2>/dev/null && echo "in use" || echo "free"; }
SPARKV=$(spark-submit --version 2>&1 | grep -oE "version [0-9]+\.[0-9]+\.[0-9]+" | head -1 | cut -d' ' -f2)
SCALAV=$(spark-submit --version 2>&1 | grep -oE "Scala version [0-9.]+" | cut -d' ' -f3)
PYSPARKV=$(python -c "import pyspark;print(pyspark.__version__)" 2>/dev/null)
HADOOPV=$(hadoop version 2>/dev/null | head -1 | cut -d' ' -f2)
HBASEV=$(hbase version 2>/dev/null | grep -oE "^HBase [0-9.]+" | cut -d' ' -f2)
SPARK_HADOOP=$(ls $SPARK_HOME/jars/hadoop-client-api-*.jar 2>/dev/null | grep -oE "[0-9]+\.[0-9]+\.[0-9]+")
{
echo "=== ParkSense environment $(date -u +%FT%TZ) ==="
echo "user: $(whoami) · $(. /etc/os-release; echo $PRETTY_NAME) · cores $(nproc) · RAM $(free -h | awk '/Mem:/{print $2}') · free disk $(df -h . | awk 'NR==2{print $4}')"
echo "java (Spark): $(java -version 2>&1 | head -1)  JAVA_HOME=$JAVA_HOME"
echo "java (Hadoop/HBase daemons): $(grep -h '^export JAVA_HOME' $HADOOP_HOME/etc/hadoop/hadoop-env.sh $HBASE_HOME/conf/hbase-env.sh | tr '\n' ' ')"
echo "spark $SPARKV (Scala $SCALAV, bundled Hadoop client $SPARK_HADOOP)  SPARK_HOME=$SPARK_HOME"
echo "sbt $(sbt --script-version 2>/dev/null)"
echo "hadoop $HADOOPV · hbase $HBASEV"
echo "jps: $(jps | grep -v Jps | awk '{print $2}' | sort | tr '\n' ' ')"
echo "hdfs: $(hdfs dfsadmin -safemode get 2>/dev/null)"; hdfs dfs -ls / 2>/dev/null | awk 'NR>1{print "  "$NF}'
echo "hbase status: $(echo status | hbase shell -n 2>/dev/null | grep -E 'active master')"
echo "hbase REST 8085: $(curl -s -m 5 -H 'Accept: application/json' localhost:8085/version/cluster || echo DOWN)"
echo "python $(python --version 2>&1 | cut -d' ' -f2) · pyspark $PYSPARKV · node $(node -v) · npm $(npm -v)"
for pt in 9000 9870 16010 9090 8085 4040 18080 5000 5173; do echo "  port $pt: $(port $pt)"; done
} 2>&1 | tee outputs/logs/env.txt
ok() { [ "$1" = 1 ] && echo PASS || echo FAIL; }
JAVA_MAJOR=$(java -version 2>&1 | grep -oE '"[0-9]+' | tr -d '"')
cat > outputs/logs/compat.md <<MD
# Compatibility checks ($(date -u +%F))

| # | Check | Found | Result |
|---|---|---|---|
| 1 | Java for Spark 3.5 (8/11/17) | $JAVA_MAJOR | $(ok $([[ $JAVA_MAJOR =~ ^(8|11|17)$ ]] && echo 1)) |
| 2 | build.sbt scalaVersion = Spark's Scala | Spark built with $SCALAV; build.sbt $(grep -oE 'scalaVersion := "[0-9.]+"' spark-scala/build.sbt 2>/dev/null) | $(ok $(grep -q "\"$SCALAV\"" spark-scala/build.sbt 2>/dev/null && echo 1)) |
| 3 | pyspark == Spark | $PYSPARKV vs $SPARKV | $(ok $([ "$PYSPARKV" = "$SPARKV" ] && echo 1)) |
| 4 | Python in Spark 3.5 range (3.8+) | $(python --version 2>&1 | cut -d' ' -f2) | PASS (3.12 works with setuptools for distutils; brief prefers 3.10/3.11) |
| 5 | Hadoop 3.x on both sides | cluster $HADOOPV, Spark client $SPARK_HADOOP | $(ok $([[ $HADOOPV == 3.* && $SPARK_HADOOP == 3.* ]] && echo 1)) |
| 6 | HBase 2.5.x with Hadoop 3.x | HBase $HBASEV, Hadoop $HADOOPV | $(ok $([[ $HBASEV == 2.5.* && $HADOOPV == 3.* ]] && echo 1)) |
| 7 | HBase client jars for Spark | \`hbase mapredcp\` lists $(hbase mapredcp 2>/dev/null | tr ':' '\n' | grep -c jar) jars | see Phase 8 writer test |
| 8 | Jackson/Guava conflicts | only \`hbase mapredcp\` jars are passed to Spark | see Phase 8 |
| 9 | Ports | see env.txt | listed |
| 10 | HBase REST JSON | $(curl -s -m 5 -H 'Accept: application/json' localhost:8085/version/cluster >/dev/null && echo up || echo down) | $(ok $(curl -s -m 5 localhost:8085/version/cluster >/dev/null && echo 1)) |

Decision: keep the installed stack (Java 8 for Hadoop/HBase daemons, Java 11 for Spark 3.5.9 / Scala 2.12.18, Python 3.12).
Spark 4.x was not used: it requires Java 17+ and Scala 2.13, and HBase 2.5 has only preliminary JDK 17 support.
MD
cat outputs/logs/compat.md
