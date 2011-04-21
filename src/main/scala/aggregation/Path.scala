package graphite.relay.aggregation

abstract class PathToken(name: String) {
  def matches(token: String): Boolean
  def substitute(tokens: Map[String, String]): String

  def matchAgainst(token: String): Option[(String, String)] = {
    if(matches(token)) Some(name → token)
    else None
  }

  override def toString = name
}

case class NamedPathToken(name: String) extends PathToken(name) {
  def matches(token: String) = (token == name)
  def substitute(tokens: Map[String, String]) = name
}

case class WildcardPathToken() extends PathToken("*") {
  def matches(token: String) = true
  def substitute(tokens: Map[String, String]) = "ANY"
}

case class GroupPathToken(name: String) extends PathToken(name) {
  def matches(token: String) = true
  def substitute(tokens: Map[String, String]) = {
    tokens(name)
  }
}

case class Path(tokens: Seq[PathToken]) {
  def length = tokens.length
  override def toString =
    "Path(%s)".format(tokens.map(_.toString).mkString("."))

  def getPath(substitutions: Map[String, String]) = {
    tokens.map(_.substitute(substitutions)).mkString(".")
  }
}
