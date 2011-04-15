package graphite.relay.backend.strategy

import javax.inject.Inject

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends


/** A backend strategy which sends all metrics to all hosts */
class Broadcast @Inject() (backends: Backends) extends BackendStrategy {
  def apply(key: String): Traversable[Backend] = backends.backends
}
