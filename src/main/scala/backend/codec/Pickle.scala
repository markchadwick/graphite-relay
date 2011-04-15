package graphite.relay.backend.codec

// import java.io.DataOutputStream
import org.jboss.netty.buffer.ChannelBuffer

class Pickle(out: ChannelBuffer) {
  type Writable = ChannelBuffer

  private var index = 0

  private val VERSION   = 0x02
  private val PROTO     = 0x80
  private val NEWOBJ    = 0x81
  private val EXT1      = 0x82
  private val EXT2      = 0x83
  private val EXT4      = 0x84
  private val TUPLE1    = 0x85
  private val TUPLE2    = 0x86
  private val TUPLE3    = 0x87
  private val NEWTRUE   = 0x88
  private val NEWFALSE  = 0x89
  private val LONG1     = 0x8a
  private val LONG4     = 0x8b
  

  private val APPENDS         = 'e'
  private val BININT          = 'J'
  private val BININT1         = 'K'
  private val BINPUT          = 'q'
  private val EMPTY_LIST      = ']'
  private val LONG_BINPUT     = 'r'
  private val MARK            = '('
  private val LONG            = 'L'
  private val SHORT_BINSTRING = 'U'
  private val FLOAT           = 'F'
  private val STOP            = '.'
  private val BINFLOAT        = 'G'

  def write(obj: AnyRef) {
    index = 1
    out.writeByte(PROTO)
    out.writeByte(VERSION)
    encodeAny(obj)
    out.writeByte(STOP)
  }

  private def encodeAny(obj: Any, putVar: Boolean=false) = {
    obj match {
      case s: String ⇒          encodeString(s, putVar)
      case l: Traversable[_] ⇒  encodeSequence(l)
      case t2: Tuple2[_, _] ⇒   encodeTuple2(t2, putVar)
      case i: Int ⇒             encodeInt(i)
      case l: Long ⇒            encodeDouble(l.doubleValue)
      case f: Float ⇒           encodeDouble(f.doubleValue)
      case d: Double ⇒          encodeDouble(d)
    }
  }

  private def encodeString(s: String, putVar: Boolean) = {
    if(s.length < 256) encodeShortString(s, putVar)
    else throw new RuntimeException("String too long for now!")
  }

  private def encodeDouble(d: Double) {
    out.writeByte(BINFLOAT)
    out.writeDouble(d)
  }

  private def encodeTuple2(tup: Tuple2[_, _], putVar: Boolean) {
    encodeAny(tup._1, false)
    encodeAny(tup._2, false)
    out.writeByte(TUPLE2)
    if(putVar) put()
  }

  private def encodeShortString(s: String, putVar: Boolean) {
    out.writeByte(SHORT_BINSTRING)
    out.writeByte(s.length)
    out.writeBytes(s.getBytes)
    if(putVar) put()
  }
 
  private def encodeSequence(l: Traversable[_]) {
    out.writeByte(EMPTY_LIST)
    put()
    out.writeByte(MARK)
    l.foreach(item ⇒ encodeAny(item))
    out.writeByte(APPENDS)
  }

  private def encodeInt(i: Int) {
    if(i < 256) {
      out.writeByte(BININT1)
      out.writeByte(i)
    } else {
      out.writeByte(BININT)
      out.writeInt(i)
    }
  }

  private def put() {
    if(index < 256) {
      out.writeByte(BINPUT)
      out.writeByte(index)
    } else {
      out.writeByte(LONG_BINPUT)
      out.writeInt(index)
    }
    index += 1
  }
}
