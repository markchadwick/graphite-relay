package graphite.relay

case class Update(metric: String, value: Double, timestamp: Long) {
  lazy val path = metric.split("\\.")
}
