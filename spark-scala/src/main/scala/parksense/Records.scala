package parksense

import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

/** One flat line: snapshot_ts \t carpark \t update_ts \t C:105:102|Y:20:20 (all SGT, no offset). */
case class FlatLine(snapshotTs: String, carpark: String, updateTs: String, lots: String)

/** One lot type of one carpark in one snapshot (times = epoch ms). */
case class Lot(carpark: String, lotType: String, snapshotTs: Long, updateTs: Long, total: Int, available: Int)

/** Lot after cleaning: clipped availability, occupancy and SGT calendar fields. */
case class LotRec(carpark: String, lotType: String, snapshotTs: Long, updateTs: Long, stalenessMin: Int,
                  total: Int, available: Int, occupancy: Double, availRatio: Double, hour: Int, minuteOfDay: Int,
                  dow: Int, isWeekend: Boolean, isHoliday: Boolean, dt: String)

case class Meta(carpark: String, carParkType: String, system: String, shortTerm: String, freeParking: String,
                nightParking: String, decks: Int, basement: String, lat: Double, lon: Double, address: String)

/** Clean occupancy record written to Parquet (partitioned by dt). */
case class Occupancy(carpark_no: String, lot_type: String, snapshot_ts: java.sql.Timestamp, update_ts: java.sql.Timestamp,
                     snapshot_ms: Long, staleness_min: Int, total_lots: Int, lots_available: Int, occupancy: Double,
                     avail_ratio: Double, hour: Int, minute_of_day: Int, dow: Int, is_weekend: Boolean, is_holiday: Boolean,
                     car_park_type: String, type_of_parking_system: String, short_term_parking: String, free_parking: String,
                     night_parking: String, car_park_decks: Option[Int], car_park_basement: String, lat: Option[Double],
                     lon: Option[Double], address: String, has_meta: Boolean, dt: String)

object Records {
  val SGT: ZoneOffset = ZoneOffset.ofHours(8)
  private val Fmt = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def sgtMillis(s: String): Long = LocalDateTime.parse(s, Fmt).toInstant(SGT).toEpochMilli
  def sgtDateTime(ms: Long): LocalDateTime = LocalDateTime.ofEpochSecond(ms / 1000, 0, SGT)

  /** Parse a flat line; Left(reason) so malformed lines are counted, never fatal. */
  def parseLine(s: String): Either[String, FlatLine] = {
    val f = s.split("\t", -1)
    if (f.length != 4) Left(s"expected 4 fields, got ${f.length}")
    else if (f(1).trim.isEmpty || f(3).trim.isEmpty) Left("empty carpark or lots")
    else try { sgtMillis(f(0)); sgtMillis(f(2)); Right(FlatLine(f(0), f(1).trim, f(2), f(3))) }
    catch { case e: Exception => Left(s"bad timestamp: ${e.getMessage}") }
  }

  /** Expand the packed lot field into one record per lot type (used inside flatMap).
    * Left = an unparseable lot item. */
  def explodeLots(fl: FlatLine): Seq[Either[String, Lot]] = {
    val snap = sgtMillis(fl.snapshotTs)
    val upd = sgtMillis(fl.updateTs)
    fl.lots.split("\\|").toSeq.map { item =>
      val p = item.split(":")
      try {
        if (p.length != 3) Left(s"bad lot item '$item'")
        else Right(Lot(fl.carpark, p(0).trim, snap, upd, p(1).trim.toInt, p(2).trim.toInt))
      } catch { case _: NumberFormatException => Left(s"bad number in '$item'") }
    }
  }

  def stalenessMin(l: Lot): Int = ((l.snapshotTs - l.updateTs) / 60000L).toInt

  /** Clip available to [0, total]; returns the record and whether clipping happened. */
  def clean(l: Lot, holidays: Set[String]): (LotRec, Boolean) = {
    val avail = math.min(math.max(l.available, 0), l.total)
    val clipped = avail != l.available
    val ratio = avail.toDouble / l.total
    val t = sgtDateTime(l.snapshotTs)
    val dow = t.getDayOfWeek.getValue // 1 = Monday
    val dt = t.toLocalDate.toString
    (LotRec(l.carpark, l.lotType, l.snapshotTs, l.updateTs, stalenessMin(l), l.total, avail, 1.0 - ratio, ratio,
      t.getHour, t.getHour * 60 + t.getMinute, dow, dow >= 6, holidays.contains(dt), dt), clipped)
  }

  /** CSV splitter that handles quoted fields with commas and escaped quotes. */
  def splitCsv(line: String): Array[String] = {
    val out = scala.collection.mutable.ArrayBuffer[String]()
    val sb = new StringBuilder
    var q = false
    var i = 0
    while (i < line.length) {
      val c = line.charAt(i)
      if (q) {
        if (c == '"' && i + 1 < line.length && line.charAt(i + 1) == '"') { sb += '"'; i += 1 }
        else if (c == '"') q = false else sb += c
      } else if (c == '"') q = true
      else if (c == ',') { out += sb.toString; sb.clear() }
      else sb += c
      i += 1
    }
    out += sb.toString
    out.toArray
  }

  /** carpark_info.csv: car_park_no,address,x_coord,y_coord,car_park_type,type_of_parking_system,short_term_parking,
    * free_parking,night_parking,car_park_decks,gantry_height,car_park_basement,lat,lon */
  def parseMeta(line: String): Option[Meta] = {
    val f = splitCsv(line)
    if (f.length < 14 || f(0) == "car_park_no") None
    else Some(Meta(f(0).trim, f(4), f(5), f(6), f(7), f(8), scala.util.Try(f(9).toDouble.toInt).getOrElse(0), f(11),
      scala.util.Try(f(12).toDouble).getOrElse(Double.NaN), scala.util.Try(f(13).toDouble).getOrElse(Double.NaN), f(1)))
  }

  def toOccupancy(r: LotRec, m: Option[Meta]): Occupancy = Occupancy(
    r.carpark, r.lotType, new java.sql.Timestamp(r.snapshotTs), new java.sql.Timestamp(r.updateTs), r.snapshotTs,
    r.stalenessMin, r.total, r.available, r.occupancy, r.availRatio, r.hour, r.minuteOfDay, r.dow, r.isWeekend, r.isHoliday,
    m.map(_.carParkType).orNull, m.map(_.system).orNull, m.map(_.shortTerm).orNull, m.map(_.freeParking).orNull,
    m.map(_.nightParking).orNull, m.map(_.decks), m.map(_.basement).orNull, m.map(_.lat), m.map(_.lon),
    m.map(_.address).orNull, m.isDefined, r.dt)

  def isValidDate(s: String): Boolean = scala.util.Try(LocalDate.parse(s)).isSuccess
}
