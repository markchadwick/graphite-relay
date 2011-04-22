package graphite.relay.backend

import java.util.concurrent.LinkedBlockingQueue
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

import org.apache.log4j.Logger
import org.jboss.netty.channel.group.DefaultChannelGroup

import graphite.relay.Update
import graphite.relay.backend.strategy.BackendStrategy
import graphite.relay.overflow.OverflowHandler


@Singleton
class BackendManager @Inject() (backends: Backends,
                                strategy: BackendStrategy,
                                overflow: OverflowHandler,
                                @Named("relay.hostbuffer") hostBuffer: Int,
                                @Named("relay.reconnect") reconnect: Int)
                               extends Thread() {
  setName("BackendManager")

  private val pendingUpdates = new LinkedBlockingQueue[Option[Update]](hostBuffer)
  private val log = Logger.getLogger(classOf[BackendManager])
  private val channels = new DefaultChannelGroup("relay-client")
  private val backendClients = Map(backends.map(newClient):_*)

  def apply(update: Update) = pendingUpdates.offer(Some(update))
  /*{
    strategy(update.metric).foreach { backend ⇒
      backendClients(backend)(update)
    }
  }*/

  override def run() = {
    log.info("Starting Up...")
   
    Stream.continually(pendingUpdates.take)
          .takeWhile(_ != None)
          .map(_.get)
          .foreach { update ⇒

      strategy(update.metric).foreach { backend ⇒
        backendClients(backend)(update)
      }
    }
 
    log.info("Shutting Down...")
    channels.close().awaitUninterruptibly()
    backendClients.values.foreach(_.shutdown())
  }

  def shutdown() = {
    pendingUpdates.put(None)
  }

  private def newClient(backend: Backend) = {
    val client =  new BackendClient(channels, backend, reconnect, overflow)
    (backend → client)
  }

  start()
}
