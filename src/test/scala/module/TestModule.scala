package graphite.relay.module

import com.google.inject.AbstractModule
import com.google.inject.name.Names

import graphite.relay.backend.Backends
import graphite.relay.backend.strategy.BackendStrategy
import graphite.relay.backend.strategy.RoundRobin
import graphite.relay.overflow.BitchingOverflowHandler
import graphite.relay.overflow.OverflowHandler


object TestModule {
  def freePort: Int = {
    import java.net.ServerSocket
    val sock = new ServerSocket(0)
    val port = sock.getLocalPort
    sock.close
    return port
  }
}

class TestModule(backends: Backends) extends AbstractModule {

  def configure() {
    bindConstant.annotatedWith(Names.named("relay.hostbuffer")).to(100)
    bindConstant.annotatedWith(Names.named("relay.port")).to(TestModule.freePort)
    bindConstant.annotatedWith(Names.named("relay.reconnect")).to(2)
    bindConstant.annotatedWith(Names.named("aggregation.config")).to("/dev/null")

    bind(classOf[BackendStrategy]).to(classOf[RoundRobin])
    bind(classOf[Backends]).toInstance(backends)
    bind(classOf[OverflowHandler]).to(classOf[BitchingOverflowHandler])
  }

}
