package graphite.relay.backend.strategy

import java.util.concurrent.atomic.AtomicInteger
import javax.inject.Inject
import scala.math.random

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends


/** Expects exactly two backends -- one that contains "cache1" that gets all
  * keys that start with "rollup_data", and some number of other ones that get
  * everything else. */
class LegacyStrategy @Inject() (backends: Backends) extends BackendStrategy {
  assert(backends.backends.length >= 2)

  private val rollups = backends.backends.find(_.host.startsWith("cache1")) match {
    case Some(cache) ⇒ cache
    case None ⇒ throw new RuntimeException("No cache1 backend")
  }

  private val defaults = backends.backends.filter(_ != rollups)

  def apply(key: String): Traversable[Backend] = {
    if(key.startsWith("rollup_data.")) List(rollups)
    else List(chooseDefault)
  }

  private def chooseDefault = defaults((random * defaults.length).toInt)
}
