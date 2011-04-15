package graphite.relay.backend

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
                                @Named("relay.reconnect") reconnect: Int) {

  private val log = Logger.getLogger(classOf[BackendManager])
  private val channels = new DefaultChannelGroup("relay-client")
  private val backendClients = Map(backends.map(newClient):_*)

  def apply(update: Update) = {
    strategy(update.metric).foreach { backend ⇒
      backendClients(backend)(update)
    }
  }

  def shutdown() = {
    log.info("Shutting Down...")
    channels.close().awaitUninterruptibly()
    backendClients.values.foreach(_.shutdown())
  }

  private def newClient(backend: Backend) = {
    val client =  new BackendClient(channels, backend, reconnect, hostBuffer,
                                    overflow)
    (backend → client)
  }
}
