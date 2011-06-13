package graphite.relay.backend

import java.net.ConnectException
import java.net.InetSocketAddress
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.channel.WriteCompletionEvent
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.util.Timeout
import org.jboss.netty.util.Timer
import org.jboss.netty.util.TimerTask

import graphite.relay.Update


class BackendClientHandler(channels: ChannelGroup, bootstrap: ClientBootstrap,  
                           timer: Timer, reconnect: Int, log: Logger)
                           extends SimpleChannelUpstreamHandler {

  private val queueSize = 10000
  private val updateQueue = new LinkedBlockingQueue[Update](queueSize)
  private var channel: Option[Channel] = None

  def isAvailable = updateQueue.remainingCapacity > 1

  def apply(update: Update) {
    updateQueue.put(update)
    channel.map(writeUpdates)
  }

  private lazy val remoteString = {
    val remote = bootstrap.getOption("remoteAddress").asInstanceOf[InetSocketAddress]
    "%s:%s".format(remote.getHostName, remote.getPort)
  }

  def connect() = {
    bootstrap.connect()
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    this.channel = None
    channels.remove(e.getChannel)
    log.info("Disconnected from %s. Reconnecting in %s seconds" .format(remoteString, reconnect)) 

    timer.newTimeout(new TimerTask {
      def run(timeout: Timeout) = connect()
    }, reconnect, TimeUnit.SECONDS)
  } 

  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val channel = e.getChannel
    channels.add(channel)

    this.channel = Some(channel)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    e.getCause match {
      case conn: ConnectException ⇒ // these are handled
      case ex ⇒ log.error(ex)
    }
  }

  /** When we think the channel is open and availble to write, pull all the
    * pending updates off the queue and write them to the channel. */
  private def writeUpdates(channel: Channel) = channel.synchronized {
    if(channel.isWritable) {
      val updates = Stream.continually(updateQueue.poll()).takeWhile(_ != null)
      if(! updates.isEmpty) {
        channel.write(updates)
      }
    }
  }
}
