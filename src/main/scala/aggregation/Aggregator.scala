package graphite.relay.aggregation

import java.util.concurrent.Executors
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton
import scala.io.Source

import org.apache.log4j.Logger

import graphite.relay.Update


/** A serivce which will load all the aggregation rules from disk, spawn off a
  * worker for each rule, and send them updates as they trickle in */
@Singleton
class Aggregator @Inject() (configParser: ConfigParser,
                            @Named("aggregation.config") config: String) {

  private val log = Logger.getLogger(classOf[Aggregator])
  private val rules: Seq[Rule] = getRules
  private val ruleThreads = rules.map(getThread)

  ruleThreads.foreach(_.start())

  /** Have this Aggregator process an update. It is not guarenteed that update
    * will be accounted for immediately */
  def apply(update: Update) = rules.foreach(_.apply(update))

  /** Cleanly shut down the rule workers. The process may hang on a shutdown
    * signal if this isn't called. */
  def shutdown() = {
    rules.foreach(_.shutdown())
    ruleThreads.foreach(_.join())
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

  private def getThread(rule: Rule) = {
    val thread = new Thread(rule)
    thread.setName(rule.toString)
    thread.setDaemon(true)
    thread
  }
}
