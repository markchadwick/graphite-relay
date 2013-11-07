package graphite.relay

import java.io.File
import java.io.FileInputStream
import java.util.Properties

import org.apache.log4j.Logger
import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.google.inject.name.Names
import scopt.OptionParser

import graphite.relay.backend.Backend
import graphite.relay.backend.Backends
import graphite.relay.backend.strategy.BackendStrategy
import graphite.relay.overflow.OverflowHandler


object Main {
  private val log = Logger.getLogger("Main")

  private var configPath: Option[String] = None
  private var log4jPath: Option[String] = None

  private var defaultBackendStrategy =
    "graphite.relay.backend.strategy.ConsistentHash"

  private var defaultOverflowHander =
    "graphite.relay.overflow.BitchingOverflowHandler"

  def main(args: Array[String]) {
    val parser = getOptionParser
    parser.parse(args)

    log4jPath.map(org.apache.log4j.PropertyConfigurator.configure)

    configPath match {
      case Some(config) ⇒
        val runtime = Runtime.getRuntime
        val injector = getInjector(config)
        val relay = injector.getInstance(classOf[GraphiteRelay])

        val relayThread = new Thread(relay)
        relayThread.start()

        runtime.addShutdownHook(new Thread {
          override def start {
            log.info("Shutting down...")
            relay.shutdown()
            relayThread.join()
          }
        })

      case None ⇒
        parser.showUsage
        System.exit(1)
    }
  }

  private def getInjector(configPath: String) = {
    val properties = new Properties()

    println("Loading %s".format(configPath))
    val propStream = new FileInputStream(new File(configPath))

    properties.load(propStream)
    propStream.close()

    Guice.createInjector(modules(properties):_*)
  }

  private def modules(properties: Properties) = {
    configModule(properties) ::
    Nil
  }

  private def configModule(properties: Properties) = new AbstractModule {
    def configure() {
      val backendStrategyClassName = get("relay.backendstrategy",
                                         defaultBackendStrategy)

      val overflowHandlerClassName = get("relay.overflowhandler",
                                         defaultOverflowHander)

      val backendStrategy = Class.forName(backendStrategyClassName)
                                 .asInstanceOf[Class[BackendStrategy]]

      val overflowHandler = Class.forName(overflowHandlerClassName)
                                 .asInstanceOf[Class[OverflowHandler]]

      val backends = getBackends(properties)

      log.info("Backend Strategy: %s".format(backendStrategy))
      log.info("Overflow Handler: %s".format(overflowHandler))

      Names.bindProperties(binder(), properties)
      bind(classOf[BackendStrategy]).to(backendStrategy)
      bind(classOf[Backends]).toInstance(backends)
      bind(classOf[OverflowHandler]).to(overflowHandler)
    }

    private def get(name: String, default: String): String =
      Option(properties.getProperty(name)).getOrElse(default)
  }

  private def getBackends(properties: Properties): Backends = {
    val backendsStr = properties.getProperty("relay.backends")
    val backendStrs = backendsStr.split("[\\n \\t]+")
                                 .map(_.trim)
                                 .filter(_ != "")
    val backends = backendStrs.map { backendStr ⇒
      val parts = backendStr.split(":")
      Backend(parts(0), parts(1).toInt, parts(2))
    }
    Backends(backends:_*)
  }

  private def getOptionParser = {
    new OptionParser("graphite-relay") {
      help("h", "help", "Display this message")
      opt("c", "config", "Path to Relay Property file", { p ⇒
        configPath = Some(p)
      })
      opt("l", "log", "Path to Log4j Config", { p ⇒
        log4jPath = Some(p)
      })
    }
  }
}
