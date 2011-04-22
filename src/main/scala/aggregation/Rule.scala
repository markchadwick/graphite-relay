package graphite.relay.aggregation

import java.util.Date
import scala.collection.mutable.Map

import graphite.relay.Update
import graphite.relay.backend.BackendManager


abstract class Rule(pubPath: Path, matchPath: Path, frequency: Long) {
  type Value
  protected def newValue: Value
  protected def applyUpdate(updateValue: Double, value: Value): Value
  protected def combine(value: Value): Double

  private var pending = Map.empty[String, Value]
  private var nextUpdate: Option[Long] = None

  /** Enqueue an update for processing. There is no guarentee it will be updated
    * immediately. If the update queue is full, this method will block. */
  def apply(update: Update) = process(update)

  /** Determine if it's a reasonable time to flush based on when the last flush
    * was performed. */
  def shouldFlush = {
    nextUpdate match {
      case None ⇒
        nextUpdate = Some(now + frequency)
        false
      case Some(update) ⇒
        update <= now
    }
  }

  /** For a given Update, transform the `pending` field to reflect the change.
    * This relies heavily on `applyUpdate`, as defined in subclasses. */
  private def process(update: Update) {
    if(update.path.length != matchPath.length) return
    val substitutions = update.path.zip(matchPath.tokens) map { case(updateTok, pathTok) ⇒
      pathTok.matchAgainst(updateTok)
    }
    if(substitutions.exists(_ == None)) return

    val subs = Map(substitutions.map(_.get):_*).toMap
    val path = pubPath.getPath(subs)

    val value = pending.get(path).getOrElse(newValue)
    pending(path) = applyUpdate(update.value, value)
  }


  /** Take all the pending updates, and flush the ones that seem far enough in
    * the past. This will happen synchronously, which should be fine, as it's in
    * its own thead, but pay attention if that's ever not the case. */
  def flush(): Traversable[Update] = {
    val ts = now
    nextUpdate = None
    pending.mapValues(combine).map { case(metric, value) ⇒
      pending -= metric
      Update(metric, value, ts)
    }
  }

  private def now = new Date().getTime / 1000L

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
  protected def applyUpdate(update: Double, value: Double) = update + value
  protected def combine(value: Double): Double = value
}


class AverageRule(pubPath: Path, matchPath: Path, frequency: Long)
                 extends Rule(pubPath, matchPath, frequency) {
  type Value = (Double, Int)

  protected def newValue = (0d, 0)
  protected def applyUpdate(update: Double, value: Value) = {
    (update + value._1, value._2 + 1)
  }

  protected def combine(value: Value): Double = {
    val (total, count) = value
    count match {
      case 0 ⇒ 0
      case _ ⇒ total / count
    }
  }
}
