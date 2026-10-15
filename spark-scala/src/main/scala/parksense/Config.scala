package parksense

import scala.io.Source

/** Minimal reader for config/config.yaml (nested maps by indentation, scalars, one-line [a, b] lists).
  * No YAML dependency, so the jar runs on any Spark distribution. Keys are dotted paths. */
class Config(val values: Map[String, String]) extends Serializable {
  def str(key: String): String = values.getOrElse(key, throw new NoSuchElementException(s"config key missing: $key"))
  def int(key: String): Int = str(key).toInt
  def double(key: String): Double = str(key).toDouble
  def bool(key: String): Boolean = str(key).toBoolean
  def list(key: String): Seq[String] =
    str(key).stripPrefix("[").stripSuffix("]").split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).filter(_.nonEmpty).toSeq

  lazy val root: String = str("project.root")
  /** Local absolute path for a key under `paths`. */
  def local(key: String): String = s"$root/${str(s"paths.$key")}"
  def useHdfs: Boolean = bool("paths.use_hdfs")
  /** Data-lake URI: hdfs:///parksense/<rel> when HDFS is on, else file://<root>/data/<rel>. */
  def lake(rel: String): String = if (useHdfs) s"${str("paths.hdfs_root")}/$rel" else s"file://$root/data/$rel"
  def metrics: String = local("metrics")
}

object Config {
  def load(file: String): Config = {
    val src = Source.fromFile(file, "UTF-8")
    val lines = try src.getLines().toList finally src.close()
    var stack = List.empty[(Int, String)]
    val out = scala.collection.mutable.LinkedHashMap[String, String]()
    for (raw <- lines) {
      val noComment = raw.replaceAll("\\s+#.*$", "")
      if (noComment.trim.nonEmpty && !noComment.trim.startsWith("#")) {
        val indent = noComment.takeWhile(_ == ' ').length
        val t = noComment.trim
        val idx = t.indexOf(':')
        if (idx > 0 && !t.startsWith("-")) {
          val k = t.substring(0, idx).trim
          val v = t.substring(idx + 1).trim
          stack = stack.dropWhile(_._1 >= indent)
          val full = (stack.reverse.map(_._2) :+ k).mkString(".")
          if (v.isEmpty) stack = (indent, k) :: stack
          else out(full) = v.stripPrefix("\"").stripSuffix("\"")
        }
      }
    }
    new Config(out.toMap)
  }
}
