package parksense.util

import org.apache.spark.{ExceptionFailure, Success => TaskSuccess}
import org.apache.spark.scheduler._

import scala.collection.mutable.ArrayBuffer

case class StageStat(stageId: Int, attempt: Int, name: String, numTasks: Int, runTimeMs: Long, inputBytes: Long,
                     shuffleReadBytes: Long, shuffleReadRecords: Long, shuffleWriteBytes: Long, shuffleWriteRecords: Long)
case class TaskEvent(stageId: Int, partitionId: Int, attempt: Int, status: String, reason: String)
case class JobStat(jobId: Int, stageIds: Seq[Int], description: String)

/** SparkListener collecting per-stage task metrics (shuffle read/write) and task failures/retries. */
class StageMetricsListener extends SparkListener {
  val stages = ArrayBuffer[StageStat]()
  val tasks = ArrayBuffer[TaskEvent]()
  val jobs = ArrayBuffer[JobStat]()
  @volatile private var jobsEnded = 0

  override def onJobStart(e: SparkListenerJobStart): Unit = synchronized {
    jobs += JobStat(e.jobId, e.stageIds, Option(e.properties).map(_.getProperty("spark.job.description", "")).getOrElse(""))
  }
  override def onJobEnd(e: SparkListenerJobEnd): Unit = synchronized { jobsEnded += 1 }
  override def onStageCompleted(e: SparkListenerStageCompleted): Unit = synchronized {
    val i = e.stageInfo
    val m = i.taskMetrics
    stages += StageStat(i.stageId, i.attemptNumber(), i.name, i.numTasks, m.executorRunTime, m.inputMetrics.bytesRead,
      m.shuffleReadMetrics.totalBytesRead, m.shuffleReadMetrics.recordsRead,
      m.shuffleWriteMetrics.bytesWritten, m.shuffleWriteMetrics.recordsWritten)
  }
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized {
    val (st, why) = e.reason match {
      case TaskSuccess => ("SUCCESS", "")
      case f: ExceptionFailure => ("FAILED", f.description)
      case o => ("FAILED", o.toString)
    }
    tasks += TaskEvent(e.stageId, e.taskInfo.partitionId, e.taskInfo.attemptNumber, st, why)
  }
  /** Listener events are asynchronous: wait until `n` jobs have ended in total. */
  def awaitJobs(n: Int, timeoutMs: Long = 20000): Unit = {
    val t0 = System.currentTimeMillis()
    while (jobsEnded < n && System.currentTimeMillis() - t0 < timeoutMs) Thread.sleep(50)
    Thread.sleep(250)
  }
  def jobsEndedCount: Int = jobsEnded
  def stagesSince(i: Int): Seq[StageStat] = synchronized { stages.drop(i).toList }
  def tasksSince(i: Int): Seq[TaskEvent] = synchronized { tasks.drop(i).toList }
}
