// ParkSense Spark jobs. scalaVersion MUST equal the Scala version Spark was built with
// (Spark 3.5.9 binary distribution -> Scala 2.12.18). Spark and HBase are "provided": compiled
// against the installed jars ($SPARK_HOME/jars and the shaded HBase client from `hbase mapredcp`),
// supplied at runtime by spark-submit (--jars), so versions can never drift.
name := "parksense"
version := "0.1"
scalaVersion := "2.12.18"

val sparkHome = sys.env.getOrElse("SPARK_HOME", sys.error("SPARK_HOME not set: source scripts/env.sh"))
val hbaseHome = sys.env.getOrElse("HBASE_HOME", sys.error("HBASE_HOME not set: source scripts/env.sh"))

Compile / unmanagedJars ++= ((file(sparkHome) / "jars") ** "*.jar").classpath
Compile / unmanagedJars ++= ((file(hbaseHome) / "lib" / "shaded-clients") ** "hbase-shaded-mapreduce-*.jar").classpath

scalacOptions ++= Seq("-deprecation", "-feature")
