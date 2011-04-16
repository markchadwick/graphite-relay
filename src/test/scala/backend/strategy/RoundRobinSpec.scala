package graphite.relay.backend.strategy

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends


class RoundRobinSpec extends FlatSpec with ShouldMatchers {
  behavior of "Round Robin"

  it should "cycle through all backends" in {
    val backend1 = Backend("x", 1)
    val backend2 = Backend("x", 2)
    val backend3 = Backend("x", 3)

    val backends = Backends(backend1, backend2, backend3)
    val hash = new RoundRobin(backends)

    hash("key") should equal (backend1 :: Nil)
    hash("key") should equal (backend2 :: Nil)
    hash("key") should equal (backend3 :: Nil)
    hash("key") should equal (backend1 :: Nil)
    hash("key") should equal (backend2 :: Nil)
    hash("key") should equal (backend3 :: Nil)
  }

  it should "handle no backends" in {
    val hash = new RoundRobin(Backends.empty)
    hash("key") should equal (Nil)
  }
}
