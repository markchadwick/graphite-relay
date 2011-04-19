package graphite.relay.backend.codec

import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.buffer.ChannelBuffers

import graphite.relay.Update


class PickleEncoder extends OneToOneEncoder {
  override def encode(ctx: ChannelHandlerContext, ch: Channel, msg: Any): AnyRef = {
    if(! msg.isInstanceOf[Seq[Update]]) return msg.asInstanceOf[AnyRef]

    val updates = msg.asInstanceOf[Seq[Update]]
    val tuples = updates.map(u ⇒ (u.metric, (u.timestamp, u.value)))

    val buf = ChannelBuffers.dynamicBuffer(128)
    val pickle = new Pickle(buf)
    pickle.write(tuples)
    return buf
  }
}
