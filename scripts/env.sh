# source scripts/env.sh — Java/Spark/Hadoop/HBase/Node/venv for hduser
export PARKSENSE_HOME=/home/hduser/parksense-sg
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64      # Spark 3.5.9 (Hadoop/HBase daemons keep Java 8 via their env files)
export SPARK_HOME=$HOME/tools/spark
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HBASE_HOME=$HOME/hbase
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$HOME/tools/sbt/bin:$HOME/tools/node/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HBASE_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$(ls $SPARK_HOME/python/lib/py4j-*-src.zip 2>/dev/null | head -1)
export PYSPARK_PYTHON=$HOME/parksense-venv/bin/python
export PYSPARK_DRIVER_PYTHON=$HOME/parksense-venv/bin/python
export SPARK_LOCAL_IP=127.0.0.1
[ -f $HOME/parksense-venv/bin/activate ] && . $HOME/parksense-venv/bin/activate
[ -f $PARKSENSE_HOME/.env ] && set -a && . $PARKSENSE_HOME/.env && set +a
