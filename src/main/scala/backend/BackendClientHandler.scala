package graphite.relay.backend

import java.net.ConnectException
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.util.Timeout
import org.jboss.netty.util.Timer
import org.jboss.netty.util.TimerTask


class BackendClientHandler(channels: ChannelGroup, bootstrap: ClientBootstrap,  
                           timer: Timer, reconnect: Int, log: Logger)
                           extends SimpleChannelUpstreamHandler {

  var channel: Option[Channel] = None

  private lazy val remoteString = {
    val remote = bootstrap.getOption("remoteAddress").asInstanceOf[InetSocketAddress]
    "%s:%s".format(remote.getHostName, remote.getPort)
  }

  def connect() = {
    bootstrap.connect()
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channel = None
    log.info("Disconnected from %s. Reconnecting in %s seconds" .format(remoteString, reconnect)) 
    timer.newTimeout(new TimerTask {
      def run(timeout: Timeout) = connect()
    }, reconnect, TimeUnit.SECONDS)
  } 

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channel = Some(e.getChannel)
    channels.add(e.getChannel)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause match {
      case conn: ConnectException ⇒ // these are handled
      case ex ⇒ log.error(ex)
    }
  }
}
