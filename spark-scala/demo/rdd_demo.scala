// ParkSense: RDD demo for the slides (read-only; run with scripts/demo_rdd.sh [day|all]).
// The same RDD operations as RddPipeline.scala, printed section by section: 1 RDD creation, 2 transformations,
// 3 actions, 4 key/value operations. Writes nothing except the saveAsTextFile example under /tmp.
// Loaded with spark-shell -i (the whole file runs as one block and exits at the end).
import java.time.{Duration, LocalDateTime}
case class Lot(snapshot: String, carpark: String, updated: String, lotType: String, total: Int, available: Int)
def banner(s: String): Unit = println(s"\n${"=" * 78}\n  $s\n${"=" * 78}")
def staleMin(l: Lot): Long = Duration.between(LocalDateTime.parse(l.updated), LocalDateTime.parse(l.snapshot)).toMinutes
def occ(l: Lot): Double = 1.0 - math.min(l.available, l.total).toDouble / l.total
def full(l: Lot): Int = if (l.available.toDouble / l.total <= 0.10) 1 else 0
val csv = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)" // split on commas outside quotes
val day = sys.env.getOrElse("DEMO_DAY", "2026-09-23")
val path = s"hdfs://localhost:9000/parksense/raw/flat/dt=${if (day == "all") "*" else day}/*.txt"

// ---------------------------------------------------------------------------------------------- 1
banner("1. RDD CREATION")
val lines = sc.textFile(path)                                                     // from files in HDFS
println(s"sc.textFile($path)")
println(s"  partitions after textFile  = ${lines.getNumPartitions}  (one per snapshot file)")
val rdd = lines.coalesce(16)                                                      // fewer, larger partitions
println(s"  partitions after coalesce  = ${rdd.getNumPartitions}")
val meta = sc.textFile("hdfs://localhost:9000/parksense/raw/meta/carpark_info.csv")
println(s"sc.textFile(carpark_info.csv): partitions = ${meta.getNumPartitions}")
val lotTypes = sc.parallelize(Seq("C" -> "car", "Y" -> "motorcycle", "H" -> "heavy vehicle"))   // from a collection
println(s"sc.parallelize(lot types)  = ${lotTypes.collect().mkString(", ")}")

// ---------------------------------------------------------------------------------------------- 2
banner("2. TRANSFORMATIONS (lazy: they only build the plan)")
val fields = rdd.map(_.split("\t", -1))                                           // map: line -> fields
val valid = fields.filter(_.length == 4)                                          // filter: well-formed lines
val lots = valid.flatMap(f => f(3).split("\\|").map(_.split(":")).filter(_.length == 3).map(p => Lot(f(0), f(1), f(2), p(0), p(1).toInt, p(2).toInt)))   // flatMap: one record per lot type
val fresh = lots.filter(l => l.total > 0 && staleMin(l) <= 30)                    // filter: drop stale / invalid
val carparks = fresh.map(_.carpark).distinct()                                    // distinct
println("map(split) -> filter(valid) -> flatMap(lot types) -> filter(fresh) -> distinct")
println("Nothing has run yet. Lineage (DAG) of the cleaned RDD, from toDebugString:")
println(fresh.toDebugString)
val cars = fresh.filter(_.lotType == "C").cache()                                 // car lots only, cache() for reuse
println(s"cache(): car-lot RDD storage level = ${cars.getStorageLevel.description}")

// ---------------------------------------------------------------------------------------------- 3
banner("3. ACTIONS (trigger the computation)")
println(f"count()        lines = ${rdd.count()}%,d   lot records = ${lots.count()}%,d   fresh = ${fresh.count()}%,d")
println(s"first()        ${cars.first()}")
println("take(5)")
cars.take(5).foreach(l => println(f"    ${l.carpark}%-6s ${l.snapshot}  ${l.available}%4d / ${l.total}%-4d free   occupancy ${occ(l) * 100}%5.1f%%"))
println(s"countByValue() lot types: ${fresh.map(_.lotType).countByValue().toSeq.sortBy(-_._2).map { case (k, v) => f"$k=$v%,d" }.mkString(", ")}")
println(s"distinct().count() carparks = ${carparks.count()}")
val largest = cars.map(l => (l.total, l.carpark)).reduce((a, b) => if (a._1 >= b._1) a else b)   // reduce: keep the larger
println(s"reduce()       largest carpark = ${largest._2} with ${largest._1} car lots")
val out = s"file:///tmp/parksense_rdd_demo_${System.currentTimeMillis}"
cars.map(l => s"${l.carpark},${l.snapshot},${l.available},${l.total}").coalesce(1).saveAsTextFile(out)
println(s"saveAsTextFile -> $out")

// ---------------------------------------------------------------------------------------------- 4
banner("4. KEY/VALUE (pair RDD) OPERATIONS")
// reduceByKey: citywide occupancy per snapshot, then reduce to the busiest moment
val city = cars.map(l => (l.snapshot, ((l.total - math.min(l.available, l.total)).toLong, l.total.toLong))).
  reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).
  mapValues { case (used, tot) => used.toDouble / tot }
val busiest = city.reduce((a, b) => if (a._2 >= b._2) a else b)
println(f"reduceByKey(snapshot) + reduce: busiest moment ${busiest._1}, citywide occupancy ${busiest._2 * 100}%.1f%%")

// aggregateByKey: mean occupancy by hour, sorted by key
val byHour = cars.map(l => (l.snapshot.substring(11, 13).toInt, occ(l))).
  aggregateByKey((0.0, 0L))((a, v) => (a._1 + v, a._2 + 1), (a, b) => (a._1 + b._1, a._2 + b._2)).
  mapValues { case (s, n) => s / n }.sortByKey().collect()
println("aggregateByKey(hour) + sortByKey: mean occupancy by hour")
byHour.grouped(6).foreach(g => println("    " + g.map { case (h, v) => f"$h%02d:00 ${v * 100}%4.1f%%" }.mkString("   ")))

// combineByKey: per-carpark min / max free lots and nearly-full rate in one pass
val stats = cars.map(l => (l.carpark, l)).combineByKey(
  (l: Lot) => (l.available, l.available, full(l).toLong, 1L),
  (c: (Int, Int, Long, Long), l: Lot) => (math.min(c._1, l.available), math.max(c._2, l.available), c._3 + full(l), c._4 + 1),
  (a: (Int, Int, Long, Long), b: (Int, Int, Long, Long)) => (math.min(a._1, b._1), math.max(a._2, b._2), a._3 + b._3, a._4 + b._4)).
  mapValues { case (mn, mx, f, n) => (mn, mx, f.toDouble / n) }

// keyBy + join: per-carpark stats with the metadata (address, type), top 5 most often nearly full
val metaKV = meta.filter(!_.startsWith("car_park_no")).map(_.split(csv, -1)).map(f => (f(0), (f(1).replace("\"", ""), f(4))))
val top5 = stats.filter { case (_, (mn, mx, _)) => mn != mx }.join(metaKV).sortBy(-_._2._1._3).take(5)   // frozen sensors (min == max) excluded
println("combineByKey(carpark) + join(metadata) + sortBy: most often nearly full (<= 10% free), frozen sensors excluded")
top5.foreach { case (cp, ((mn, mx, rate), (addr, _))) => println(f"    $cp%-6s full ${rate * 100}%5.1f%% of readings   free lots $mn%d-$mx%d   $addr") }

// groupByKey vs reduceByKey: same answer (max free lots per carpark), groupByKey shuffles every value
val g = cars.map(l => (l.carpark, l.available)).groupByKey().mapValues(_.max)
val r = cars.map(l => (l.carpark, l.available)).reduceByKey(math.max)
println(s"groupByKey vs reduceByKey (max free lots per carpark): same result = ${g.join(r).filter { case (_, (a, b)) => a != b }.isEmpty()}")

// broadcast variable + countByKey: readings per carpark type without a shuffle join
val bMeta = sc.broadcast(metaKV.collectAsMap())
val byType = cars.map(l => (bMeta.value.get(l.carpark).map(_._2).getOrElse("UNKNOWN"), 1)).countByKey()
println("broadcast(metadata) + countByKey: readings per carpark type")
byType.toSeq.sortBy(-_._2).foreach { case (t, n) => println(f"    $t%-34s $n%,10d") }
cars.unpersist()
println("\nDone.")
System.exit(0)
