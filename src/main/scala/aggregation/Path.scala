package graphite.relay.aggregation

import graphite.relay.Update


sealed abstract class PathToken(val repr: String) {
  def matches(token: String): Boolean
}

case class NamedPathToken(name: String) extends PathToken(name) {
  def matches(token: String) = (token == name)
}

case class WildcardPathToken() extends PathToken("*") {
  def matches(token: String) = true
}

case class GroupPathToken(name: String) extends PathToken("<%s>".format(name)) {
  def matches(token: String) = true
}

class Path(val path: Seq[PathToken]) {
  def groups = path.filter(_.isInstanceOf[GroupPathToken])

  def get(subs: Map[String, String]): Seq[String] = {
    path.map(_ match {
      case NamedPathToken(name) ⇒ name
      case GroupPathToken(group) ⇒ subs(group)
      case _ ⇒ "X"
    })
  }

  def matches(tokens: Seq[String]): Option[Map[String, String]] = {
    if(tokens.size != path.size) return None
    if(path.zip(tokens).exists { case(p, t) ⇒ ! p.matches(t) } ) return None
 
    val translated = path.zip(tokens) map { case(p, token) ⇒
      p match {
        case GroupPathToken(group) ⇒ (group → token)
        case _ ⇒ (token → token)
      }
    }

    Some(Map(translated:_*))
  }

  override def toString = 
    "Path(%s)".format(path.map(_.repr).mkString("."))
}
