package graphite.relay.backend.strategy

import java.security.MessageDigest
import javax.inject.Inject
import javax.inject.Named
import scala.collection.SortedMap

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends


/** Chose a backend based on a consistent hashing algorithm. When the number of
  * backends changes, you may have holes in your data, but this should keep it
  * to a minimum. */
class ConsistentHash @Inject() (backends: Backends,
                                @Named("hash.replicas") replicas: Int)
                               extends BackendStrategy {

  private val circle = getCircle

  /** Get an appropriate backend given a key. This should only return `Nil` if
    * no backends have been specified, which isn't going to make this too useful
    * of a relay, now is it? */
  def apply(key: String): Traversable[Backend] = {
    if(circle.isEmpty) return Nil

    val keyHash = hash(key)
    circle.get(keyHash) match {
      case Some(backend) ⇒ List(backend)
      case None ⇒
        val tailMap = circle.from(keyHash)
        if(tailMap.isEmpty) List(circle.head._2) else List(tailMap.head._2)
    }
  }

  /** Hash a given string in proper Graphite compatible format */
  private def hash(string: String): Long = {
    val hash = MessageDigest.getInstance("MD5").digest(string.getBytes)
    return Integer.parseInt(hash.map("%02X".format(_)).mkString.slice(0,4), 16)
  }

  /** Initialize the hashing circle basted on the backends this class was
    * constructed with. */
  private def getCircle = {
    val backendKeys = backends.map { backend ⇒
      (0 to replicas).map { i ⇒
        hash("%s:%s".format(backend.toInstanceString, i)) → backend
      }
    }
    SortedMap[Long, Backend](backendKeys.flatten:_*)
  }
}
