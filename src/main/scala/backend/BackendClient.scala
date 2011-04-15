package graphite.relay.backend

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue

import org.apache.log4j.Logger
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender
import org.jboss.netty.util.HashedWheelTimer

import graphite.relay.Update
import graphite.relay.overflow.OverflowHandler


class BackendClient(channels: ChannelGroup, backend: Backend, reconnect: Int,
                    hostBuffer: Int, overflow: OverflowHandler) {

  private val log = Logger.getLogger(toString)
  private val updateQueue = new LinkedBlockingQueue[Update](hostBuffer)
  private val timer = new HashedWheelTimer()
  timer.start()

  private val bootstrap = newBootstrap
  private val address = new InetSocketAddress(backend.host, backend.port)
  private val handler = new BackendClientHandler(channels, bootstrap, timer, reconnect, log)
  
  bootstrap.setPipelineFactory(newPipleFactory)
  bootstrap.setOption("remoteAddress", address)

  handler.connect()

  def apply(update: Update) {
    handler.channel match {
      case Some(channel) ⇒
        // Drain the queue if we need to 
        Stream.continually(updateQueue.poll)
              .takeWhile(_ != null)
              .foreach(u ⇒ handleUpdate(u, channel))

        handleUpdate(update, channel)

      case None ⇒
        handleHostDown(update)
    }
  }

  def shutdown() = bootstrap.releaseExternalResources()

  /*
   * (lp0
   * (S'metric'
   * p1
   * (F123.0
   * F456.0
   * tp2
   * tp3
   * a.
   */
  private def handleUpdate(update: Update, channel: Channel) = {
    val pickled = "(lp0\n(S'%s'\np1\n(F%s\nF%s\ntp2\ntp3\na.\n".format(
      update.metric, update.value, update.timestamp).getBytes
    val buffer = ChannelBuffers.copiedBuffer(pickled)
    channel.write(buffer)
  }

  private def handleHostDown(update: Update) = {
    if(updateQueue.size >= hostBuffer) {
      overflow(update)
    } else {
      updateQueue.put(update)
    }
  }


  private def newBootstrap = new ClientBootstrap(
    new NioClientSocketChannelFactory(
      Executors.newFixedThreadPool(numCores),
      Executors.newFixedThreadPool(numCores)))

  private def numCores = Runtime.getRuntime.availableProcessors

  private def newPipleFactory = new ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline
     
      pipeline.addLast("encoder", new LengthFieldPrepender(4))
      pipeline.addLast("handler", handler)

      pipeline
    }
  }

  override def toString =
    "Backend[%s:%s]".format(backend.host, backend.port)
}
