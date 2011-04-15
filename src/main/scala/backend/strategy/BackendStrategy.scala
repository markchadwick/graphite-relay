package graphite.relay.backend.strategy

import graphite.relay.backend.Backend


trait BackendStrategy {
  def apply(key: String): Traversable[Backend]
}
