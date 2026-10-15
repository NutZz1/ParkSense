package parksense.util

import java.io.{File, FileWriter}
import java.time.Instant

/** time(label)(block) -> (result, ms); every timing is appended to outputs/metrics/timings.csv. */
object Timer {
  @volatile var timingsFile: String = "outputs/metrics/timings.csv"
  @volatile var job: String = "unknown"

  def time[T](label: String)(block: => T): (T, Long) = {
    val t0 = System.nanoTime()
    val result = block
    val ms = (System.nanoTime() - t0) / 1000000L
    append(label, ms)
    println(f"    [time] $label%-60s $ms%9d ms")
    (result, ms)
  }

  private def append(label: String, ms: Long): Unit = synchronized {
    val f = new File(timingsFile)
    f.getParentFile.mkdirs()
    val isNew = !f.exists()
    val w = new FileWriter(f, true)
    try {
      if (isNew) w.write("job,label,ms,recorded_at_utc\n")
      val q = "\""
      w.write(job + "," + q + label.replace(q, "'") + q + "," + ms + "," + Instant.now() + "\n")
    } finally w.close()
  }
}

/** Driver-side writers for small (already collected) results and console banners. */
object Out {
  private def esc(v: Any): String = {
    val s = v match {
      case None | null => ""
      case Some(x) => x.toString
      case d: Double => if (d.isNaN) "" else f"$d%.6f".replaceAll("0+$", "").replaceAll("\\.$", "")
      case x => String.valueOf(x)
    }
    if (s.contains(",") || s.contains("\"") || s.contains("\n")) "\"" + s.replace("\"", "\"\"") + "\"" else s
  }

  def csv(path: String, header: Seq[String], rows: Seq[Seq[Any]]): Unit = {
    val f = new File(path)
    f.getParentFile.mkdirs()
    val w = new FileWriter(f, false)
    try { w.write(header.mkString(",") + "\n"); rows.foreach(r => w.write(r.map(esc).mkString(",") + "\n")) }
    finally w.close()
    println(s"    -> $path (${rows.size} rows)")
  }

  def text(path: String, content: String): Unit = {
    val f = new File(path)
    f.getParentFile.mkdirs()
    val w = new FileWriter(f, false)
    try w.write(content) finally w.close()
    println(s"    -> $path")
  }

  def banner(title: String): Unit = {
    println(); println("=" * 96); println(s"  $title"); println("=" * 96)
  }
}
