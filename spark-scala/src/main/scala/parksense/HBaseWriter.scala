package parksense

import java.time.{OffsetDateTime, ZoneOffset}

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{BufferedMutator, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

/** Plain HBase client API (no hbase-spark connector). Connections are NOT serialisable, so they are
  * created inside foreachPartition on the executor (or once on the driver for small writes).
  * hbase-site.xml comes from --files / --driver-class-path. */
object HBaseWriter {
  val NS = "parksense"

  def connect(): Connection = ConnectionFactory.createConnection(HBaseConfiguration.create())

  private def nowIso: String = OffsetDateTime.now(ZoneOffset.ofHours(8)).toString

  /** metrics table: row = metric name, v:json + v:updated_at. */
  def metricPut(name: String, json: String): Put =
    new Put(Bytes.toBytes(name)).addColumn(Bytes.toBytes("v"), Bytes.toBytes("json"), Bytes.toBytes(json))
      .addColumn(Bytes.toBytes("v"), Bytes.toBytes("updated_at"), Bytes.toBytes(nowIso))

  def writeMetrics(conn: Connection, rows: Iterator[(String, String)]): Int = {
    val m: BufferedMutator = conn.getBufferedMutator(TableName.valueOf(s"$NS:metrics"))
    var n = 0
    try rows.foreach { case (k, v) => m.mutate(metricPut(k, v)); n += 1 } finally { m.flush(); m.close() }
    n
  }

  /** Called inside foreachPartition: one connection per partition, BufferedMutator batches the puts. */
  def writeMetricsPartition(rows: Iterator[(String, String)]): Unit = {
    val conn = connect()
    try writeMetrics(conn, rows) finally conn.close()
  }

  /** carpark_live:s rows (carpark, total, available, occupancy, update_ts, snapshot_ts). */
  def writeLiveBatch(rows: Iterator[(String, Int, Int, Double, String, String)]): Unit = {
    val conn = connect()
    val m = conn.getBufferedMutator(TableName.valueOf(s"$NS:carpark_live"))
    val s = Bytes.toBytes("s")
    try rows.foreach { case (cp, tot, av, occ, upd, snap) =>
      m.mutate(new Put(Bytes.toBytes(cp)).addColumn(s, Bytes.toBytes("total"), Bytes.toBytes(tot.toString))
        .addColumn(s, Bytes.toBytes("available"), Bytes.toBytes(av.toString)).addColumn(s, Bytes.toBytes("occupancy"), Bytes.toBytes(f"$occ%.4f"))
        .addColumn(s, Bytes.toBytes("update_ts"), Bytes.toBytes(upd)).addColumn(s, Bytes.toBytes("snapshot_ts"), Bytes.toBytes(snap)))
    } finally { m.flush(); m.close(); conn.close() }
  }
}
