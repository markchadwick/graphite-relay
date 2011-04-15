package graphite.relay.backend.strategy

import java.util.concurrent.atomic.AtomicInteger
import javax.inject.Inject

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends

class RoundRobin @Inject() (backends: Backends) extends BackendStrategy {
  private val numBackends = backends.backends.size
  private val maxIndex = numBackends - 1
  private val current = new AtomicInteger()

  def apply(key: String): Traversable[Backend] = {
    val count = current.incrementAndGet()
    if(count > maxIndex) current.set(0)

    val backendIndex = count % (numBackends)
    List(backends.backends(backendIndex))
  }
}
