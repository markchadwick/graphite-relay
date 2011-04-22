package graphite.relay.aggregation

import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton
import scala.io.Source

import org.apache.log4j.Logger

import graphite.relay.Update
import graphite.relay.backend.BackendManager


/** A serivce which will load all the aggregation rules from disk, spawn off a
  * worker for each rule, and send them updates as they trickle in */
@Singleton
class Aggregator @Inject() (backendManager: BackendManager,
                            configParser: ConfigParser,
                            @Named("aggregation.config") config: String)
                           extends Runnable {

  private var droppedUpdates = 0L
  private var queuedUpdates = 0L
  private val updateQueue = new LinkedBlockingQueue[Option[Update]](10000)
  private val log = Logger.getLogger(classOf[Aggregator])
  private val rules: Seq[Rule] = getRules

  /** Have this Aggregator process an update. It is not guarenteed that update
    * will be accounted for immediately */
  def apply(update: Update) = updateQueue.offer(Some(update))

  /** Cleanly shut down the rule workers. The process may hang on a shutdown
    * signal if this isn't called. */
  def shutdown() = {
    updateQueue.put(None)
  }

  def run = {
    log.info("Aggregator starting up...")
    Stream.continually(updateQueue.take)
          .takeWhile(_ != None)
          .map(_.get)
          .foreach(update ⇒
            rules.foreach(rule ⇒ processUpdate(rule, update)))
    log.info("Aggregator shut down")
  }

  private def processUpdate(rule: Rule, update: Update) = {
    rule.apply(update)

    if(rule.shouldFlush) {
      val updates = rule.flush()
      updates.foreach(println)
      updates.foreach(update ⇒ backendManager(update))
    }
  }

  /** Load the rules off disk and spawn each one in a worker thread. This will
    * simply expode and die if the config is invalid. */
  private def getRules = {
    val rules = configParser(Source.fromFile(config).mkString).get
    rules foreach { rule ⇒
      log.info("Loaded rule: %s".format(rule))
    }
    rules
  }
}
