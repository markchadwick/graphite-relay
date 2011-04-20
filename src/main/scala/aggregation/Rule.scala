package graphite.relay.aggregation

import graphite.relay.Update


abstract sealed class Rule(_pubPath: Path, _matchPath: Path, val frequency: Long) {
  validate()

  def apply(update: Update) = {
    _matchPath.matches(update.path) match {
      case None ⇒ // No match
      case Some(params) ⇒
        val metric = _pubPath.get(params).mkString(".")
        addUpdate(Update(metric, update.value, update.timestamp))
    }
  }

  /** Take a new Update and add it to the current state of this rule. It is safe
    * to assume that all updates passed to this method will match the match
    * path. It is also safe to assume that this method will execute serially in
    * exactly one thread. */
  def addUpdate(update: Update)

  def flush(): Traversable[Update]

  /** Bail early if this Rule is invalid. For example, if the pub path
    * references a group that isn't in the match path, we'll just throw errors
    * later. */
  private def validate() = {
    _pubPath.path.find(_.isInstanceOf[WildcardPathToken]) match {
      case None ⇒ // no pub wildcards
      case Some(wc) ⇒
         throw new IllegalArgumentException("Can't wildcard a publish path")
    }

    val matchGroups = _matchPath.groups.map(_.repr)
    val pubGroups = _pubPath.groups.map(_.repr)

    val mismatches = pubGroups.filter(! matchGroups.contains(_))
    if(! mismatches.isEmpty) {
      val message = "Error in %s. Groups %s not defined".format(this, mismatches)
      throw new IllegalArgumentException(message)
    }
  }

  override def toString =
    "%s(%s (%s) = %s)".format(getClass, _pubPath, frequency, _matchPath)
}


case class SumRule(pubPath: Path, matchPath: Path, override val frequency: Long)
                  extends Rule(pubPath, matchPath, frequency) {
  /** Map[metric, Map[timebucket, sum]] */
  private var updates = Map.empty[String, Map[Long, Double]]

  /** Given an update, find the time bucket is, increment the bucket, and go
    * about our business */
  def addUpdate(update: Update) = {
    val timeBuckets = updates.get(update.metric)
                             .getOrElse(Map.empty[Long, Double])

    val bucket = (update.timestamp / frequency).toLong
    val sum = timeBuckets.get(bucket)
                         .map(_ + update.value)
                         .getOrElse(update.value)

    updates += (update.metric → (timeBuckets + (bucket → sum)))
  }

  def flush() = {
    val now = (new java.util.Date().getTime / 1000).longValue + frequency

    val updates = for {
      (metric, buckets) ← this.updates;
      (ts, sum) ← buckets
    } yield Update(metric, sum, ts * frequency)

    val pastUpdates = updates.filter(_.timestamp < now)
    // Reset Counters
    pastUpdates foreach { update ⇒
      this.updates(update.metric) += ((update.timestamp / frequency) → 0)
    }
    this.updates = Map.empty[String, Map[Long, Double]]
    pastUpdates
  }
}


case class AverageRule(pubPath: Path, matchPath: Path, override val frequency: Long)
                      extends Rule(pubPath, matchPath, frequency) {
  def addUpdate(update: Update) = {
  }

  def flush() = Nil
}
