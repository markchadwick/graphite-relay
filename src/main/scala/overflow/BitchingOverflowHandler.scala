package graphite.relay.overflow

import java.util.Date
import javax.inject.Singleton

import org.apache.log4j.Logger

import graphite.relay.Update


/** Overflow handler that simply complains to a log and hopes that you, one day,
  * will pay attention to it. You never pay attention to it. */
@Singleton
class BitchingOverflowHandler extends OverflowHandler {
  private val maxBitch = 1000
  private val log = Logger.getLogger(classOf[BitchingOverflowHandler])
  private var lastBitch: Long = 0

  def apply(update: Update) = {
    if(canBitch) {
      log.warn("Can't store %s".format(update))
    }
  }

  private def canBitch: Boolean = {
    val now = (new Date).getTime
    if(now > (lastBitch + maxBitch)) {
      lastBitch = now
      return true
    } else {
      return false
    }
  }
}
