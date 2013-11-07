package graphite.relay.server

import javax.inject.Singleton
import javax.inject.Inject

import java.lang.NumberFormatException

import org.apache.log4j.Logger

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

import graphite.relay.backend.BackendManager
import graphite.relay.Update


@Singleton
class RelayUpdateHandler @Inject() (backendManager: BackendManager)
                                   extends SimpleChannelUpstreamHandler {

  private val log = Logger.getLogger(classOf[RelayUpdateHandler])

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage().toString()
    val parts = message.split(" ")
    if(parts.length == 3) {
      try {
        val update = Update(parts(0), parts(1).toDouble, parts(2).trim().toLong)
        backendManager(update)
      } catch {
        case ex: NumberFormatException => {}
      }
    } else {
      // sorry, we don't care about this sh*t in logs
      // TODO: make proper switch for loggig invalid msgs
      //log.error("Invalid Message %s".format(e))
      closeOnFlush(e.getChannel())
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    log.error(e.getCause)
    closeOnFlush(e.getChannel())
  }

  private def closeOnFlush(channel: Channel) {
    if(channel.isConnected()) {
      channel.write(ChannelBuffers.EMPTY_BUFFER)
             .addListener(ChannelFutureListener.CLOSE)
    }
  }
}
