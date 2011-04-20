package graphite.relay.aggregation

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers


class ConfigParserTest extends FlatSpec with ShouldMatchers {
  behavior of "Config Parser"

  it should "parse a simple config entry" in {
    val parser = new ConfigParser()
    val config = "<exchange>.bid_requests (60) = sum foo.*.bar"

    val rule = parser(config).get(0).asInstanceOf[SumRule]

    rule.pubPath.path should equal (List(GroupPathToken("exchange"),
                                         NamedPathToken("bid_requests")))

    rule.frequency should equal (60)

    rule.matchPath.path should equal (List(NamedPathToken("foo"),
                                           WildcardPathToken(),
                                           NamedPathToken("bar")))
  }

  it should "parse a multi-line config" in {
    val parser = new ConfigParser()

    val config = """
      |<exchange>.bid_requests       (60)  = sum server.<exchange>.*.bid_request
      |<exchange>.timeouts           (70)  = sum server.<exchange>.*.timeouts
      |<exchange>.avg_response_time  (80)  = avg server.<exchange>.*.rt
    """.stripMargin

    val rules = parser(config).get
    rules should have size (3)

    // Ensure correct ordering
    rules(0).asInstanceOf[SumRule].frequency should equal (60)
    rules(1).asInstanceOf[SumRule].frequency should equal (70)
    rules(2).asInstanceOf[AverageRule].frequency should equal (80)
  }

  it should "parse a config with blank lines" in {
    val parser = new ConfigParser()

    val config = """
      |
      |<exchange>.bid_requests       (60)  = sum server.<exchange>.*.bid_request
      |
      |<exchange>.timeouts           (70)  = sum server.<exchange>.*.timeouts
      |
      |<exchange>.avg_response_time  (80)  = avg server.<exchange>.*.rt
      |
    """.stripMargin

    val rules = parser(config).get
    rules should have size (3)
  }

  it should "parse a config with comments" in {
    val parser = new ConfigParser()

    val config = """
      |#
      |# Here be my exchange timeouts
      |#
      |<exchange>.bid_requests       (60)  = sum server.<exchange>.*.bid_request
      |<exchange>.timeouts           (70)  = sum server.<exchange>.*.timeouts
      |<exchange>.avg_response_time  (80)  = avg server.<exchange>.*.rt
      | # one.two (60) = avg three.four
    """.stripMargin

    val rules = parser(config).get
    rules should have size (3)
  }

  it should "throw on syntax errors" in {
    val parser = new ConfigParser()
    val config = "one.two = sum foo.bar"
    pending
  }
}
