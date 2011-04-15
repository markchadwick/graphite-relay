package graphite.relay.overflow

import java.io.File
import java.io.FileOutputStream
import java.io.OutputStream
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.LinkedBlockingQueue
import java.util.zip.GZIPOutputStream
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

import org.apache.log4j.Logger

import graphite.relay.Update


@Singleton
class LoggingOverflowHandler @Inject() (@Named("overflow.directory") overflowDir: String)
                                       extends OverflowHandler {

  case class LogFile(stream: OutputStream)

  private val log = Logger.getLogger(classOf[LoggingOverflowHandler])
  private val overflow = new File(overflowDir)  
  private val updates = new LinkedBlockingQueue[Option[Update]](10000)
  private val MAX_UPDATES = 1000000
  private lazy val thread = getThread
  private var numUpdates = 0
  private val logFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-S")
  private var currentLog: Option[LogFile] = None

  overflow.mkdirs()
  roll()
  thread.start()

  def apply(update: Update) = updates.put(Some(update))


  private def logUpdate(update: Update) = {
    if(numUpdates >= MAX_UPDATES) roll()

    val repr = "%s %s %s\n".format(update.metric, update.value, update.timestamp)
    currentLog.map(_.stream.write(repr.getBytes))
    numUpdates += 1
  }

  private def roll() = {
    currentLog.map(_.stream.close())
    currentLog = Some(newLogFile)
    numUpdates = 0
  }

  private def newLogFile: LogFile = {
    val now = new Date()
    val filename = "overflow-%s.gz".format(logFormat.format(now))
    log.info("Rolling to %s".format(filename))
    val file = new File(overflow, filename)
    LogFile(new GZIPOutputStream(new FileOutputStream(file)))
  }

  private def getThread = {
    val thread = new Thread(getRunnable)
    thread.setName("Logging Overflow Handler")
    thread
  }

  private def getRunnable = new Runnable {
    def run = {
      Stream.continually(updates.take)
                    .takeWhile(_ != None)
                    .map(_.get)
                    .foreach(logUpdate)
      log.info("Shut Down cleanly")
    }
  }

  private def registerShutdownHook =
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def start = {
        updates.put(None)
      }
    })
}
