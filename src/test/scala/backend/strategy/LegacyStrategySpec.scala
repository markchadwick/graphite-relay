package graphite.relay.backend.strategy

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends

class LegacyStrategySpec extends FlatSpec with ShouldMatchers {
  behavior of "Legacy Strategy"

  it should "refuse to initialize with less than two backends" in {
    intercept[AssertionError] {
      new LegacyStrategy(Backends.empty)
    }

    intercept[AssertionError] {
      val backend1 = Backend("one", 1)
      new LegacyStrategy(Backends(backend1))
    }
  }

  it should "refuse to initialize without a cache1 backend" in {
    val backends = Backend("one", 1) :: Backend("two", 2) :: Nil

    intercept[RuntimeException] {
      new LegacyStrategy(Backends(backends:_*))
    }
  }

  it should "send rollups to cache1" in {
    val cache = Backend("cache1.host", 1)
    val other = Backend("other.host", 1)
    val strategy = new LegacyStrategy(Backends(cache, other))

    strategy("some.key") should be (other :: Nil)
    strategy("rollup_data.one") should be (cache :: Nil)
    strategy("rollup_dataone") should be (other :: Nil)
  }
}
