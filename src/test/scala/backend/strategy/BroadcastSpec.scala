package graphite.relay.backend.strategy

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends


class BroadcastSpec extends FlatSpec with ShouldMatchers {
  behavior of "Broadcast"

  it should "send all updates to all backends" in {
    val backends = Backend("x", 1) :: Backend("x", 2) :: Nil
    val hash = new Broadcast(Backends(backends:_*))

    val res = hash("any")
    res should have size (2)
  }
}
