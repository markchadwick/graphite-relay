import sbt._

class Plugins(info: sbt.ProjectInfo) extends PluginDefinition(info) {
  val codaRepo = "Coda Hale's Repository" at "http://repo.codahale.com/"
  val scctRepo = "scct-repo" at "http://mtkopone.github.com/scct/maven-repo/"

  lazy val assemblySBT = "com.codahale" % "assembly-sbt" % "0.1.1"
  lazy val scctPlugin = "reaktor" % "sbt-scct-for-2.8" % "0.1-SNAPSHOT"
}
