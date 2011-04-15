package graphite.relay.overflow

import graphite.relay.Update


trait OverflowHandler {
  def apply(update: Update) 
}
