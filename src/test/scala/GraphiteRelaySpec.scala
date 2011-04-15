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

  /*
  it should "bind to a port" in {
    val injector = getInjector()

    withServer(injector) { relay ⇒
      val socket = new Socket("localhost", relay.port)
      val out = socket.getOutputStream()
      out.write("metric.one 123 1234567890\n".getBytes)
      out.close()
    }
  }

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
      Thread.sleep(1000)
      dummyBackends.foreach { backend ⇒
        println("%s (%s)".format(backend.asBackend, backend.lines.size))
      }
    }
  }
  */
}
