package graphite.relay.backend

case class Backend(host: String, port: Int) {
  override def toString = "%s:%s".format(host, port)
}
