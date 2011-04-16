package graphite.relay.backend.codec

import java.io.DataOutputStream

import org.jboss.netty.buffer.ChannelBuffer


object Pickle {
  type Writable = {
    def writeByte(int: Int)
    def writeInt(int: Int)
    def writeDouble(double: Double)
  }
}


class Pickle(out: Pickle.Writable) {
  def this(o: ChannelBuffer) = this(o.asInstanceOf[Pickle.Writable])
  def this(o: DataOutputStream) = this(o.asInstanceOf[Pickle.Writable])

  private var index = 0

  private val MARK            = '('   // push special markobject on stack
  private val STOP            = '.'   // every pickle ends with STOP
  private val POP             = '0'   // discard topmost stack item
  private val POP_MARK        = '1'   // discard stack top through topmost markobject
  private val DUP             = '2'   // duplicate top stack item
  private val FLOAT           = 'F'   // push float object; decimal string argument
  private val INT             = 'I'   // push integer or bool; decimal string argument
  private val BININT          = 'J'   // push four-byte signed int
  private val BININT1         = 'K'   // push 1-byte unsigned int
  private val LONG            = 'L'   // push long; decimal string argument
  private val BININT2         = 'M'   // push 2-byte unsigned int
  private val NONE            = 'N'   // push None
  private val PERSID          = 'P'   // push persistent object; id is taken from string arg
  private val BINPERSID       = 'Q'   //  "       "         "  ;  "  "   "     "  stack
  private val REDUCE          = 'R'   // apply callable to argtuple, both on stack
  private val STRING          = 'S'   // push string; NL-terminated string argument
  private val BINSTRING       = 'T'   // push string; counted binary string argument
  private val SHORT_BINSTRING = 'U'   //  "     "   ;    "      "       "      " < 256 bytes
  private val UNICODE         = 'V'   // push Unicode string; raw-unicode-escaped'd argument
  private val BINUNICODE      = 'X'   //   "     "       "  ; counted UTF-8 string argument
  private val APPEND          = 'a'   // append stack top to list below it
  private val BUILD           = 'b'   // call __setstate__ or __dict__.update()
  private val GLOBAL          = 'c'   // push self.find_class(modname, name); 2 string args
  private val DICT            = 'd'   // build a dict from stack items
  private val EMPTY_DICT      = '}'   // push empty dict
  private val APPENDS         = 'e'   // extend list on stack by topmost stack slice
  private val GET             = 'g'   // push item from memo on stack; index is string arg
  private val BINGET          = 'h'   //   "    "    "    "   "   "  ;   "    " 1-byte arg
  private val INST            = 'i'   // build & push class instance
  private val LONG_BINGET     = 'j'   // push item from memo on stack; index is 4-byte arg
  private val LIST            = 'l'   // build list from topmost stack items
  private val EMPTY_LIST      = ']'   // push empty list
  private val OBJ             = 'o'   // build & push class instance
  private val PUT             = 'p'   // store stack top in memo; index is string arg
  private val BINPUT          = 'q'   //   "     "    "   "   " ;   "    " 1-byte arg
  private val LONG_BINPUT     = 'r'   //   "     "    "   "   " ;   "    " 4-byte arg
  private val SETITEM         = 's'   // add key+value pair to dict
  private val TUPLE           = 't'   // build tuple from topmost stack items
  private val EMPTY_TUPLE     = ')'   // push empty tuple
  private val SETITEMS        = 'u'   // modify dict by adding topmost key+value pairs
  private val BINFLOAT        = 'G'   // push float; arg is 8-byte float encoding

  private val VERSION         = 0x02
  private val PROTO           = 0x80  // identify pickle protocol
  private val NEWOBJ          = 0x81  // build object by applying cls.__new__ to argtuple
  private val EXT1            = 0x82  // push object from extension registry; 1-byte index
  private val EXT2            = 0x83  // ditto, but 2-byte index
  private val EXT4            = 0x84  // ditto, but 4-byte index
  private val TUPLE1          = 0x85  // build 1-tuple from stack top
  private val TUPLE2          = 0x86  // build 2-tuple from two topmost stack items
  private val TUPLE3          = 0x87  // build 3-tuple from three topmost stack items
  private val NEWTRUE         = 0x88  // push True
  private val NEWFALSE        = 0x89  // push False
  private val LONG1           = 0x8a  // push long from < 256 bytes
  private val LONG4           = 0x8b  // push really big long


  def write(obj: AnyRef) {
    index = 1
    out.writeByte(PROTO)
    out.writeByte(VERSION)
    encodeAny(obj, true)
    out.writeByte(STOP)
  }

  private def encodeAny(obj: Any, putVar: Boolean=false) = {
    obj match {
      case null ⇒               out.writeByte(NONE)
      case s: String ⇒          encodeString(s, putVar)
      case l: Traversable[_] ⇒  encodeSequence(l)
      case i: Int ⇒             encodeInt(i)
      case l: Long ⇒            encodeDouble(l.doubleValue)
      case f: Float ⇒           encodeDouble(f.doubleValue)
      case d: Double ⇒          encodeDouble(d)
      case b: Boolean ⇒         encodeBoolean(b)

      case t1: Tuple1[_] ⇒        encodeTuple1(t1, putVar)
      case t2: Tuple2[_, _] ⇒     encodeTuple2(t2, putVar)
      case t3: Tuple3[_, _, _] ⇒  encodeTuple3(t3, putVar)
      case tn: Product ⇒          encodeTupleN(tn, putVar)
    }
  }

  private def encodeString(s: String, putVar: Boolean) = {
    if(s.length < 256) encodeShortString(s, putVar)
    else encodeLongString(s, putVar)
  }

  private def encodeBoolean(b: Boolean) {
    if(b) out.writeByte(NEWTRUE) else out.writeByte(NEWFALSE)
  }

  private def encodeDouble(d: Double) {
    out.writeByte(BINFLOAT)
    out.writeDouble(d)
  }

  private def encodeTuple1(tup: Tuple1[_], putVar: Boolean) {
    encodeAny(tup._1, false)
    out.writeByte(TUPLE1)
    if(putVar) put()
  }

  private def encodeTuple2(tup: Tuple2[_, _], putVar: Boolean) {
    encodeAny(tup._1, false)
    encodeAny(tup._2, false)
    out.writeByte(TUPLE2)
    if(putVar) put()
  }

  private def encodeTuple3(tup: Tuple3[_, _, _], putVar: Boolean) {
    encodeAny(tup._1, false)
    encodeAny(tup._2, false)
    encodeAny(tup._3, false)
    out.writeByte(TUPLE3)
    if(putVar) put()
  }

  private def encodeTupleN(tupn: Product, putVar: Boolean) {
    /*
    encodeAny(tup._1, false)
    encodeAny(tup._2, false)
    encodeAny(tup._3, false)
    out.writeByte(TUPLE3)
    if(putVar) put()
    */
  }

  private def encodeShortString(s: String, putVar: Boolean) {
    out.writeByte(SHORT_BINSTRING)
    out.writeByte(s.length)
    s.getBytes.map(_.intValue).foreach(out.writeByte)
    if(putVar) put()
  }

  private def encodeLongString(s: String, putVar: Boolean) {
    out.writeByte(BINSTRING)
    out.writeInt(s.length)
    s.getBytes.map(_.intValue).foreach(out.writeByte)
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
    if(i >= 0) {
      if(i <= 0xff) {
        out.writeByte(BININT1)
        out.writeByte(i)
      } else if(i <= 0xffff) {
        out.writeByte(BININT2)
        out.writeByte(i & 0xff)
        out.writeByte(i >> 8)
      }
    } else {
      val highBits = i >> 31
      if(highBits == 0 || highBits == -1) {
        out.writeByte(BININT)

        out.writeByte((i >> 8) & 0xff)
        out.writeByte(i & 0xff)

        out.writeByte((i >> 24) & 0xff)
        out.writeByte((i >> 16) & 0xff)
      } else {
        // write as string?!
        throw new RuntimeException("Won't write a string repr!")
      }
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
