package graphite.relay.aggregation

import java.util.Date
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.Map

import graphite.relay.Update


abstract class Rule(pubPath: Path, matchPath: Path, frequency: Long)
                   extends Runnable {
  type Value
  protected def newValue: Value
  protected def applyUpdate(updateValue: Double, value: Value): Value
  protected def toUpdate(metric: String, value: Value, ts: Long): Option[Update]

  private type PendingKey = (String, Long)
  private val updateQueue = new LinkedBlockingQueue[Option[Update]](5000)
  private var pending = Map.empty[PendingKey, Value]
  private var nextUpdate: Option[Long] = None
  private val updateLag = frequency * 3


  /** Enqueue an update for processing. There is no guarentee it will be updated
    * immediately. If the update queue is full, this method will block. */
  def apply(update: Update) = {
    if(updateQueue.remainingCapacity > 0) {
      updateQueue.put(Some(update))
    } else {
      println("%s dropping %s".format(this, update))
    }
  }

  /** As long as no one has shut down this rule, continue to take updates from
    * the queue in a blocking fashion to update them. */
  def run() = {
    println("Rule[%s] starting up...".format(this))
    Stream.continually(updateQueue.take)
          .takeWhile(_ != None)
          .map(_.get)
          .foreach(process)
    println("Rule[%s] shutting down...".format(this))
  }

  /** Eventually shuts down the Rule after all pending events have been
    * processed. Any updates enqueued after this call will be ignored. */
  def shutdown() = updateQueue.put(None)

  /** For a given Update, transform the `pending` field to reflect the change.
    * This relies heavily on `applyUpdate`, as defined in subclasses. */
  private def process(update: Update) {
    if(shouldFlush) flush()

    if(update.path.length != matchPath.length) return
    val substitutions = update.path.zip(matchPath.tokens) map { case(updateTok, pathTok) ⇒
      pathTok.matchAgainst(updateTok)
    }
    if(substitutions.exists(_ == None)) return

    val subs = Map(substitutions.map(_.get):_*)
    val path = pubPath.getPath(subs.toMap)

    val key = (path, timestampToKey(update.timestamp))
    val value = pending.get(key).getOrElse(newValue)

    pending(key) = applyUpdate(update.value, value)
  }

  private def timestampToKey(timestamp: Long) = timestamp / frequency
  private def keyToTimestamp(key: Long) = key * frequency

  /** Determine if it's a reasonable time to flush based on when the last flush
    * was performed. */
  private def shouldFlush = {
    val now = new Date().getTime / 1000
    nextUpdate match {
      case None ⇒
        nextUpdate = Some(now + frequency)
        false
      case Some(update) ⇒
        update <= now
    }
  }

  /** Take all the pending updates, and flush the ones that seem far enough in
    * the past. This will happen synchronously, which should be fine, as it's in
    * its own thead, but pay attention if that's ever not the case. */
  private def flush() = {
    val cutoff = (new Date().getTime / 1000) - updateLag

    println("--------------------- UPDATE --------------------------")
    println("NOW:    %s".format(new Date().getTime / 1000))
    println("CUTUFF: %s".format(cutoff))
    val updates = pending filter { case(key, value) ⇒
      keyToTimestamp(key._2) < cutoff
    }

    // Remove updates from pending
    updates.foreach { case(key, _) ⇒ pending -= key }

    // Transform them back into Updates
    val updatesToPush = updates.map({ case(key, value) ⇒
      toUpdate(key._1, value, keyToTimestamp(key._2))       
    }).flatten

    nextUpdate = None 
    updatesToPush.foreach(println) 
  }

  /** Human-readble format to display this rule as. at. */
  override def toString = 
    "%s(%s (%s) = %s)".format(getClass.getName, pubPath, frequency, matchPath)
}


/** A rule which will sum up all values for matched updates, and simply return
  * the total value seen for that metric in the given time bucket */
class SumRule(pubPath: Path, matchPath: Path, frequency: Long)
             extends Rule(pubPath, matchPath, frequency) {
  type Value = Double

  protected def newValue = 0d
  protected def applyUpdate(update: Double, value: Double) =
    update + value

  protected def toUpdate(metric: String, value: Double, ts: Long) =
    Some(Update(metric, value, ts))
}


class AverageRule(pubPath: Path, matchPath: Path, frequency: Long)
                 extends Rule(pubPath, matchPath, frequency) {
  type Value = (Double, Int)
  protected def newValue = (0d, 0)
  protected def applyUpdate(update: Double, value: Value) = {
    (update + value._1, value._2 + 1)
  }

  protected def toUpdate(metric: String, value: Value, ts: Long) = {
    val (total, count) = value
    count match {
      case 0 ⇒ None
      case c ⇒ Some(Update(metric, total/count, ts))
    }
  }
}
