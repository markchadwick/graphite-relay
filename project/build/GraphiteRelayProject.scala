import sbt._
import assembly.AssemblyBuilder

class GraphiteRelayProject(info: ProjectInfo) extends DefaultProject(info) 
                                              with AssemblyBuilder {
  lazy val jbossRepo = Repositories.jboss
  lazy val nexus = Repositories.nexusSnapshots

  lazy val netty    = Dependencies.netty
  lazy val guice    = Dependencies.guice
  lazy val jodaTime = Dependencies.jodaTime
  lazy val scopt    = Dependencies.scopt
  lazy val log4j    = Dependencies.log4j

  lazy val scalatest = Dependencies.scalatest % "test"

  object Repositories {
    def jboss = "JBoss Repo" at
      "http://repository.jboss.org/nexus/content/groups/public/"

    def nexusSnapshots = "Nexus Snapshots" at
      "http://nexus.scala-tools.org/content/repositories/snapshots/"
  }

  object Dependencies {
    def guice = "com.google.inject" % "guice" % "3.0"
    def jodaTime = "joda-time" % "joda-time" % "1.6.2"
    def log4j = "log4j" % "log4j" % "1.2.16"
    def netty = "org.jboss.netty" % "netty" % "3.2.4.Final"
    def scalatest = "org.scalatest" % "scalatest" % "1.3"
    def scopt = "com.github.scopt" %% "scopt" % "1.0.0-SNAPSHOT"
  }
}
