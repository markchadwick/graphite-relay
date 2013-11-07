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

    val numRuns = 1000
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

  it should "return an empty list with no backends" in {
    val hash = new ConsistentHash(Backends.empty, 10)
    hash("one") should equal (Nil)
    hash("two") should equal (Nil)
    hash(null) should equal (Nil)
  }

  it should "work when asking for a direct hash hit" in {
    val backend = Backend("localhost", 123, "a")
    val backends = Backends(backend)
    val hash = new ConsistentHash(backends, 1)
   
    val key = "%s:0".format(backend.toInstanceString)
    hash(key) should equal (List(backend))
  }

  it should "comparing hashing results with Graphite web hashing" in {
    val metric = "test.metric"
    val backendA = Backend("127.0.0.1", 2101, "a")
    val backendB = Backend("127.0.0.1", 2201, "b")
    val backendC = Backend("127.0.0.1", 2301, "c")
    val backendD = Backend("127.0.0.1", 2401, "d")
    val backends = Backends(backendA,backendB,backendC,backendD)
    // 100 - is built in value for Graphite web
    val hash = new ConsistentHash(backends, 100)
    hash(metric) should equal (List(backendB))
  }

  private def getBackends(count: Int) = {
    (0 to count).map(i ⇒ Backend("localhost", i))
  }
}
