package graphite.relay.aggregation

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import graphite.relay.Update


class RuleSpec extends FlatSpec with ShouldMatchers {
  behavior of "Rule"

  it should "refuse to create a rule published to a wildcard" in {
    val pubPath = new Path(List(WildcardPathToken()))
    val matchPath = new Path(List(WildcardPathToken()))

    intercept[IllegalArgumentException] {
      SumRule(pubPath, matchPath, 60)
    }
  }

  it should "refuse to create a rule with mismatched groups" in {
    val pubPath = new Path(List(GroupPathToken("known"),
                                GroupPathToken("unknown")))
    val matchPath = new Path(List(GroupPathToken("known")))

    intercept[IllegalArgumentException] {
      SumRule(pubPath, matchPath, 60)
    }
  }

  it should "match only valid metrics" in {
    val pubPath = new Path(List(NamedPathToken("one")))
    val matchPath = new Path(List(NamedPathToken("two")))

    val rule = SumRule(pubPath, matchPath, 60)

    rule(Update("bad", 123, 1234567890))
    rule(Update("two.bad", 123, 1234567890))
    rule(Update("two", 123, 1234567860))

    val updates = rule.flush().toSeq
    updates should have size (1)

    updates(0) should equal (Update("one", 123, 1234567860))

    rule.flush() should have size (0)
  }

  it should "sub by time bucket" in {
    val pubPath = new Path(List(NamedPathToken("any")))
    val matchPath = new Path(List(NamedPathToken("one")))

    val rule = SumRule(pubPath, matchPath, 60)

    rule(Update("one", 111, 1234567860))
    rule(Update("one", 222, 1234567890))
    rule(Update("one", 222, 1234500000))

    val updates = rule.flush().toSeq
    updates should have size (2)

    updates(0).value should equal(333)
    updates(1).value should equal(222)
  }

  it should "substitute group tokens" in {
    // should translate "hi.steve" ⇒ "name.steve"
    val pubPath = new Path(List(NamedPathToken("name"),
                                GroupPathToken("name")))

    val matchPath = new Path(List(NamedPathToken("hi"),
                                  GroupPathToken("name")))

    val rule = SumRule(pubPath, matchPath, 60)

    rule(Update("hi.bob", 1, 600))
    rule.flush().head should equal (Update("name.bob", 1, 600))
  }
}
