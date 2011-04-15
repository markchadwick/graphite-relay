package graphite.relay

import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

import org.apache.log4j.Logger
import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.ChannelFactory
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory

import graphite.relay.backend.BackendManager
import graphite.relay.server.RelayPipelineFactory


@Singleton
class GraphiteRelay @Inject() (relayPipelineFactory: RelayPipelineFactory,
                               backendManager: BackendManager,
                               @Named("relay.port") val port: Int)
                              extends Runnable {

  private val log = Logger.getLogger(classOf[GraphiteRelay])
  private val startupLock = new Semaphore(0)
  private val shutdownLock = new Semaphore(0)

  private val channels = new DefaultChannelGroup("relay-server")
  private lazy val factory = newChannelFactory
  private lazy val bootstrap = newBootstrap(factory)

  log.info("Running on %s cores".format(numCores))

  def run() = {
    start()
    waitForShutdown()
    stop()
  }

  def shutdown() = shutdownLock.release()

  def waitForStartup() = startupLock.acquire()

  private def waitForShutdown() = {
    startupLock.release()
    shutdownLock.acquire()
  }

  protected def start() = {
    log.info("Starting Up...")
    val channel = bootstrap.bind(new InetSocketAddress(port))
    channels.add(channel)
  }

  protected def stop() = {
    log.info("Shutting Down...")
    channels.close().awaitUninterruptibly()
    factory.releaseExternalResources()
    backendManager.shutdown()
  }

  private def newChannelFactory = new NioServerSocketChannelFactory(
    Executors.newFixedThreadPool(numCores),
    Executors.newFixedThreadPool(numCores))

  private def newBootstrap(channelFactory: ChannelFactory) = {
    val bootstrap = new ServerBootstrap(channelFactory)
    bootstrap.setPipelineFactory(relayPipelineFactory)
    bootstrap 
  }

  private def numCores = Runtime.getRuntime.availableProcessors
}
