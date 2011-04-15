package graphite.relay.backend.strategy

import java.util.UUID
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends

class ConsistentHashSpec extends FlatSpec with ShouldMatchers {
  behavior of "Consistent Hashing"

  it should "not move many keys on backend change" in {
    val backendsBefore = Backends(getBackends(100):_*)
    val backendsAfter = backendsBefore.drop(1)

    val hashA = new ConsistentHash(backendsBefore, 10)
    val hashB = new ConsistentHash(backendsAfter, 10)

    val numRuns = 5000
    val different = (0 to numRuns).foldLeft(0) { case (numDifferent, _) ⇒
      val key = UUID.randomUUID.toString
      val backendA = hashA(key)
      val backendB = hashB(key)

      if(backendA == backendB) {
        numDifferent
      } else {
        numDifferent + 1
      }
    }

    val ratio = different.toFloat / numRuns
    ratio should be < (0.02f) // should be ~ 0.01
  }

  private def getBackends(count: Int) = {
    (0 to count).map(i ⇒ Backend("localhost", i))
  }
}
