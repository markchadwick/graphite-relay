package graphite.relay.aggregation

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton
import java.util.Date
import scala.io.Source

import org.apache.log4j.Logger

import graphite.relay.Update


@Singleton
class Aggregator @Inject() (@Named("aggregation.config") config: String,
                            configParser: ConfigParser) {
  private val log = Logger.getLogger(classOf[Aggregator])

  private val updateQueue = new LinkedBlockingQueue[Option[Update]](10000)
  private lazy val rules = getRules
  private lazy val thread = getThread(loop)
  private var nextFlushes = Map.empty[Rule, Long]

  log.info("Loading Aggregator!")
 
  def apply(update: Update) = updateQueue.put(Some(update))

  private def loop() {
    log.info("Aggregator running...")
    var running = true
    while(running) {
      running = updateRules()
      flushRules()
    }
    log.info("Aggregator dunbar!")
  }

  def start() = thread.start()
  def stop() = {
    updateQueue.put(None)
    thread.join()
  }

  private def updateRules(): Boolean = {
    val updates = Stream.continually(updateQueue.poll())
                        .takeWhile(_ != null)

    updates.foreach(_ match {
      case None ⇒ return false
      case Some(update) ⇒ rules.foreach(_.apply(update))
    })
    true
  }

  private def flushRules() {
    val now = (new Date().getTime / 1000).longValue
    rules foreach { rule ⇒
      nextFlushes.get(rule) match {
        case None ⇒ nextFlushes += (rule → (now + rule.frequency))

        case Some(flushTime) if (flushTime <= now) ⇒
          flushRule(rule)
          nextFlushes += (rule → (now + rule.frequency))

        case _ ⇒ // nothing to do
      }
    }
  }

  private def flushRule(rule: Rule) {
    val now = (new Date().getTime / 1000).longValue
    log.info("NOW: %s".format(now))
    rule.flush().foreach { update ⇒
      log.info(update.toString)
    }
  }

  private def getRules = {
    val rules = configParser(Source.fromFile(config).mkString).get
    rules.foreach { rule ⇒
      log.info("Loaded Rule: %s".format(rule))
    }
    rules
  }

  private def getThread(runnable: () ⇒ Any) = {
    val thread = new Thread(new Runnable { def run = runnable() })
    thread.setDaemon(true)
    thread.setName("Aggregator")
    thread
  }
}
