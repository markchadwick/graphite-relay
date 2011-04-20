package graphite.relay.server

import javax.inject.Singleton
import javax.inject.Inject

import org.apache.log4j.Logger

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

import graphite.relay.Update
import graphite.relay.aggregation.Aggregator
import graphite.relay.backend.BackendManager


@Singleton
class RelayUpdateHandler @Inject() (backendManager: BackendManager,
                                    aggregator: Aggregator)
                                   extends SimpleChannelUpstreamHandler {

  private val log = Logger.getLogger(classOf[RelayUpdateHandler])

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val message = e.getMessage().toString()
    val parts = message.split(" ")
    if(parts.length == 3) {
      val update = Update(parts(0), parts(1).toDouble, parts(2).trim().toLong)
      backendManager(update)
      aggregator(update)
    } else {
      log.error("Invalid Message %s".format(e))
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
