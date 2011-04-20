package graphite.relay.aggregation

import scala.util.parsing.combinator._


class ConfigParser extends RegexParsers {

  def apply(s: String) = parse(rules, s)

  def rules: Parser[Seq[Rule]] = rep(configEntry) ^^ { entries ⇒
    entries.filter(_.isInstanceOf[Rule]).map(_.asInstanceOf[Rule])
  }

  def configEntry = rule | comment

  def comment = """#.*\n""".r

  def rule: Parser[Rule] = path ~ frequency ~ "=" ~ ("sum" | "avg" ) ~ path ^^ {
    case pub ~ freq ~ "=" ~ "sum" ~ pattern ⇒ SumRule(pub, pattern, freq)
    case pub ~ freq ~ "=" ~ "avg" ~ pattern ⇒ AverageRule(pub, pattern, freq)
  }

  def path: Parser[Path] = repsep(pathToken, ".") ^^ { tokens ⇒
    new Path(tokens)
  }

  def pathToken: Parser[PathToken] =
    namedPathToken | 
    wildcardPathToken  |
    groupPathToken

  def namedPathToken = ident ^^ { name ⇒
    NamedPathToken(name)
  }


  def wildcardPathToken = "*" ^^ { name ⇒
    WildcardPathToken()
  }

  def groupPathToken = "<" ~> ident <~ ">" ^^ { name ⇒
    GroupPathToken(name)
  }

  def frequency: Parser[Int] = "(" ~> numericLit <~ ")" 

  def ident = """[a-zA-Z_0-9]+""".r
  def numericLit = """[0-9]+""".r ^^ { _.toInt }
}
