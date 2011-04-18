package graphite.relay

import java.net.Socket
import java.util.UUID

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import graphite.relay.backend.Backends

class GraphiteRelaySpec extends FlatSpec
                        with ShouldMatchers
                        with ServerTests {
  behavior of "Graphite Relay"

  it should "route traffic to a backend" in {
    withBackends(3) { dummyBackends ⇒
      val backends = Backends(dummyBackends.map(_.asBackend):_*)
      val injector = getInjector(backends = backends)

      withServer(injector) { relay ⇒
        val socket = new Socket("localhost", relay.port)
        val out = socket.getOutputStream()

        (0 to 10000).foreach { i ⇒
          val metric = "metric.%s".format(UUID.randomUUID.toString)
          val update = "%s 123 1234567890\n".format(metric)
          out.write(update.getBytes)
        }
        out.close()
      }
      Thread.sleep(500)
      dummyBackends.foreach { backend ⇒
        backend.messages should be > (500)
      }
    }
  }

  it should "parse \\r\\n delimited lines" in {
    withBackends(3) { dummyBackends ⇒
      val backends = Backends(dummyBackends.map(_.asBackend):_*)
      val injector = getInjector(backends = backends)

      withServer(injector) { relay ⇒
        val socket = new Socket("localhost", relay.port)
        val out = socket.getOutputStream()

        (0 to 10000).foreach { i ⇒
          val metric = "metric.%s".format(UUID.randomUUID.toString)
          val update = "%s 123 1234567890\r\n".format(metric)
          out.write(update.getBytes)
        }
        out.close()
      }
      Thread.sleep(500)
      dummyBackends.foreach { backend ⇒
        backend.messages should be > (500)
      }
    }
  }
}
