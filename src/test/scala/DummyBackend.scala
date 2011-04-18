package graphite.relay

import java.io.DataInputStream
import java.net.ServerSocket
import java.net.SocketTimeoutException

import graphite.relay.backend.Backend
import graphite.relay.module.TestModule


class DummyBackend extends Runnable {
  val timeout = 100
  val port = TestModule.freePort
  val socket = new ServerSocket(port)
  var running = true
  var messages: Int = 0
  socket.setSoTimeout(timeout)

  def asBackend = Backend("localhost", port)

  def run = while(running) {
    try {
      val client = socket.accept()
      val in = new DataInputStream(client.getInputStream)

      try {
        var readingMessages = true
        while(readingMessages) {
          try {
            val length = in.readInt()
            in.skipBytes(length)
            messages += 1
          } catch {
            case _ ⇒ readingMessages = false
          }
        }
      } finally {
        in.close()
        client.close()
      }
    } catch {
      case ex: SocketTimeoutException ⇒ // ignore
    }
  }
}
