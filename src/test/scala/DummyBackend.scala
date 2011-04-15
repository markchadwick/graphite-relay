package graphite.relay

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.net.SocketTimeoutException

import graphite.relay.backend.Backend
import graphite.relay.module.TestModule


class DummyBackend extends Runnable {
  val timeout = 100
  val port = TestModule.freePort
  val socket = new ServerSocket(port)
  var running = true
  var lines: List[String] = Nil
  socket.setSoTimeout(timeout)

  def asBackend = Backend("localhost", port)

  def run = while(running) {
    try {
      val client = socket.accept()
      val in = new BufferedReader(new InputStreamReader(client.getInputStream))

      try {
        Stream.continually(in.readLine)
              .takeWhile(_ != null)
              .foreach(line ⇒ lines ::= line) 
      } finally {
        in.close()
        client.close()
      }
    } catch {
      case ex: SocketTimeoutException ⇒ // ignore
    }
  }
}
