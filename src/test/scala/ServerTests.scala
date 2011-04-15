package graphite.relay

import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.google.inject.Injector

import graphite.relay.backend.Backends
import graphite.relay.module.TestModule


/** Utilities to help with tests that want to test something against a running
  * server. */
trait ServerTests {

  def withServer(injector: Injector)(func: GraphiteRelay ⇒ Any) {
    val server = injector.getInstance(classOf[GraphiteRelay])
    val serverThread = new Thread(server)

    serverThread.start()
    server.waitForStartup()

    try {
      func(server)
    } finally {
      server.shutdown()
      serverThread.join()
    }
  }

  def withBackends(count: Int)(func: Seq[DummyBackend] ⇒ Any) {
    val dummies = (0 to count).map(i ⇒ new DummyBackend())
    val threads = dummies.map(d ⇒ new Thread(d))

    threads.foreach(_.start)
    func(dummies)
    dummies.foreach(_.running = false)
    threads.foreach(_.join)
  }

  def getInjector(backends: Backends=Backends.empty): Injector = {
    val modules = new TestModule(backends) :: Nil
    Guice.createInjector(modules:_*)
  }
}
