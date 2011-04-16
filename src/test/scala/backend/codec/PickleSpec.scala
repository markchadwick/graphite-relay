package graphite.relay.backend.codec

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

class PickleSpec extends FlatSpec with ShouldMatchers {
  behavior of "Pickle"

  shouldConvert("a small integer", 123, "123")

  shouldConvert("true", true, "True")

  shouldConvert("false", false, "False")

  shouldConvert("null", null, "None")

  shouldConvert("negative one", -1, "-1")

  // shouldConvert("a negative integer", -123, "-123")

  shouldConvert("zero", 0, "0")

  shouldConvert("a short string", "hello", "'hello'")

  shouldConvert("a tuple1", new Tuple1(1), "(1,)")

  shouldConvert("a tuple2", ("a", "b"), "('a', 'b')")

  shouldConvert("a nested tuple 2", ("a", ("b", "c")), "('a', ('b', 'c'))")

  shouldConvert("another nested tuple 2", (("a", "b"), "c"),
                "(('a', 'b'), 'c')")

  shouldConvert("a simple list", List(1, 2, 3), "[1, 2, 3]")

  shouldConvert("a tuple3", (1, 2, 3), "(1, 2, 3)")

  // shouldConvert("a long tuple", (1, 2, 3, 4, 5), "(1, 2, 3, 4, 5)")

  shouldConvert("a convert a homogonous list", List(1, "a", false),
                "[1, 'a', False]")

  private def shouldConvert(label: String, jvmValue: Any, pyRepr: String) {
    it should "convert %s for pickle".format(label) in {
      val pickledValue = pickled(jvmValue)
      val cmd = """
        |import pickle
        |import sys
        |r = pickle.load(sys.stdin)
        |sys.stdout.write("%r" % (r,))
      """.stripMargin

      val bytes = pyExecute(cmd, pickledValue)
      new String(bytes, "UTF-8") should equal (pyRepr)
    }

    it should "convert %s for cPickle".format(label) in {
      val pickledValue = pickled(jvmValue)
      val cmd = """
        |import cPickle
        |import sys
        |r = cPickle.load(sys.stdin)
        |sys.stdout.write("%r" % (r,))
      """.stripMargin

      val bytes = pyExecute(cmd, pickledValue)
      new String(bytes, "UTF-8") should equal (pyRepr)
    }
  }

  private def pickled(jvmValue: Any): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val stream = new DataOutputStream(bytes)
    val pickle = new Pickle(stream)
    pickle.write(jvmValue.asInstanceOf[AnyRef])
    stream.flush()

    bytes.toByteArray
  }

  private def pyExecute(cmd: String, stdin: Array[Byte]): Array[Byte] = {
    val proc = new ProcessBuilder("python", "-c", cmd)
                      .redirectErrorStream(true)
                      .start()
    val in = proc.getOutputStream()
    in.write(stdin, 0, stdin.length)
    in.close()

    val stdout = proc.getInputStream()
    val out = Stream.continually(stdout.read())
                    .takeWhile(_ > -1)
                    .map(_.toByte)

    return out.toArray
  }
}
