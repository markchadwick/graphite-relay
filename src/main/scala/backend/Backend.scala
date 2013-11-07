package graphite.relay.backend

case class Backend(host: String, port: Int, instance: String = "") {
  override def toString = "%s:%s".format(host, port)
  def toInstanceString = "('%s', '%s')".format(host, instance)
}
