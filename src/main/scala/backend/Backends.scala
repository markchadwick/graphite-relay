package graphite.relay.backend

import com.google.inject.Singleton

object Backends {
  def apply(b: Backend*) = new Backends {
    override val backends = b.toSeq
  }

  def empty = new Backends
}

@Singleton
class Backends {
  val backends: Seq[Backend] = Nil
  def map[U](func: Backend ⇒ U) = backends.map(func)
  def drop(i: Int) = Backends(backends.drop(i):_*)
}
