package graphite.relay.backend.codec

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

class PickleSpec extends Spec with ShouldMatchers {

  describe("Primatives") {
    shouldBeBinaryEqual("a small integer", 123, "123")
    shouldBeBinaryEqual("true", true, "True")
    shouldBeBinaryEqual("false", false, "False")
    shouldBeBinaryEqual("null", null, "None")
    shouldBeBinaryEqual("negative one", -1, "-1")
    shouldBeBinaryEqual("negative two", -2, "-2")
    shouldBeBinaryEqual("a negative integer", -123, "-123")
    shouldBeBinaryEqual("zero", 0, "0")
    shouldBeBinaryEqual("a float", 3.5f, "3.5")
    shouldBeBinaryEqual("a negative float", -3.5f, "-3.5")
  }

  describe("Strings") {
    shouldBeBinaryEqual("a short string", "hello", "'hello'")

    val longString = "*" * 260
    shouldBeBinaryEqual("a long string", longString, "'%s'".format(longString))
  }

  describe("Tuples") {
    // empty tuple...?
    shouldBeBinaryEqual("a tuple1", new Tuple1(1), "(1,)")

    shouldConvert("a tuple2", ("a", "b"), "('a', 'b')")

    shouldConvert("a nested tuple 2", ("a", ("b", "c")), "('a', ('b', 'c'))")

    shouldConvert("another nested tuple 2", (("a", "b"), "c"),
                  "(('a', 'b'), 'c')")

    shouldConvert("a tuple3", (1, 2, 3), "(1, 2, 3)")

    shouldConvert("a long tuple", ("one", 2, 3, 4, (5, 5, 5)),
                  "('one', 2, 3, 4, (5, 5, 5))")
  }

  describe("Lists") {
    shouldConvert("a simple list", List(1, 2, 3), "[1, 2, 3]")

    shouldConvert("an emptly list", Nil, "[]")

    shouldConvert("a convert a homogonous list", List(1, "a", false),
                  "[1, 'a', False]")
  }

  private def shouldBeBinaryEqual(label: String, jvmValue: Any, pyRepr: String) {
    shouldConvert(label, jvmValue, pyRepr)

    it("should be binary equal to pickle with %s".format(label)) {
      val cmd = """
        |import pickle
        |import sys
        |sys.stdout.write(pickle.dumps(%s, 2))
      """.stripMargin.format(pyRepr)
      val pyBytes = pyExecute(cmd, Array.empty)
      val jvmBytes = pickled(jvmValue)

      if(pyBytes.toList != jvmBytes.toList) {

        println("Python: %s".format(toHex(pyBytes)))
        println("JVM:    %s".format(toHex(jvmBytes)))
        
        jvmBytes should equal (pyBytes)
      }
    }

  }

  private def shouldConvert(label: String, jvmValue: Any, pyRepr: String) {
    it("should convert %s pickle readable".format(label)) {
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

    it("should convert %s cPickle readable".format(label)) {
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

  private def toHex(bs: Array[Byte]) =
    bs.map(b ⇒ "%02x".format(b)).mkString(" ")
}
