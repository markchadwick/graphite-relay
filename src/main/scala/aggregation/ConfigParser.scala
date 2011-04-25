package graphite.relay.aggregation

import scala.util.parsing.combinator._


/** A simple parser that will read Graphite-style aggregaction configs in the
  * format:
  *  metric.&lt;wildcard&gt;.foo (60) = sum other.&lt;wildcard&gt;.*
  */
class ConfigParser extends RegexParsers {

  /** Optionally parse a configuration into a set of Rule instances. This will
    * return a parse result -- it will be `get`-able if the syntax is valid,
    * otherwise an error message. */
  def apply(config: String) = parse(rules, config)

  /** Set of of rules which will either be comments or rule entries. It will
    * filter out any non-rule line */
  private def rules: Parser[Seq[Rule]] = rep(ruleOrComment) ^^ { entries ⇒
    entries.filter(_.isInstanceOf[Rule])
           .map(_.asInstanceOf[Rule])
  }

  /** Parse a line, which is expected to either be a rule or a comment. Blank
    * lines are ignored. */
  private def ruleOrComment = rule | comment

  /** Everything from a `#` to the end of the line should be considered a
    * comment. */
  private def comment = """#.*\n""".r

  /** Find a series of tokens that looks like a `Rule` and instantiate it. */
  private def rule: Parser[Rule] =
    path ~ frequency ~ "=" ~ ("sum" | "avg") ~ path ^^ {
      case pub ~ freq ~ "=" ~ "sum" ~ path ⇒ new SumRule(pub, path, freq)
      case pub ~ freq ~ "=" ~ "avg" ~ path ⇒ new AverageRule(pub, path, freq)
    }

  /** Parse a path as a series of `.` separated path tokens */
  private def path: Parser[Path] = repsep(pathToken, ".") ^^ { tokens ⇒
    new Path(tokens)
  }

  /** Define a "path token" as either a named, wildcard, or group pattern, with
    * presidence in that order. */
  private def pathToken: Parser[PathToken] =
    namedPathToken |
    wildcardPathToken |
    groupPathToken

  private def namedPathToken = ident ^^ { name ⇒ NamedPathToken(name) }
  private def wildcardPathToken = "*" ^^ { name ⇒ WildcardPathToken() }
  private def groupPathToken = "<" ~> ident <~ ">" ^^ { name ⇒ GroupPathToken(name) }

  private def frequency: Parser[Long] = "(" ~> numericLit <~ ")"
  private def ident = """[a-zA-Z_0-9]+""".r
  private def numericLit = """[0-9]+""".r ^^ { _.toLong }
}
