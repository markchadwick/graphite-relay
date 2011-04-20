package graphite.relay.aggregation

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

import graphite.relay.Update


class PathSpec extends FlatSpec with ShouldMatchers {
  behavior of "Path"

  it should "match equal paths" in {
    val path = new Path(List(NamedPathToken("one"),
                             NamedPathToken("two"),
                             NamedPathToken("three")))
    path.matches(List("one", "two", "three")) should not be (None)
    path.matches(List("one", "two")) should be (None)
    path.matches(List("one", "two", "three", "four")) should be (None)
  }

  it should "translate matched path tokens" in {
    val path = new Path(List(NamedPathToken("one"),
                             GroupPathToken("two"),
                             GroupPathToken("three")))

    val params = path.matches("one.bar.baz".split("\\."))
    println(params)
  }
}
