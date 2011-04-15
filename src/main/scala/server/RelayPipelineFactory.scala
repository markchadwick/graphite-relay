package graphite.relay.server

import javax.inject.Inject
import javax.inject.Singleton

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.ChannelPipeline
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.socket.ClientSocketChannelFactory
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder
import org.jboss.netty.handler.codec.frame.Delimiters
import org.jboss.netty.handler.codec.string.StringDecoder
import org.jboss.netty.handler.codec.string.StringEncoder


@Singleton
class RelayPipelineFactory @Inject() (handler: RelayUpdateHandler)
                                     extends ChannelPipelineFactory {

  def getPipeline = {
    val pipeline = Channels.pipeline()

    pipeline.addLast("framer",  getFramer)
    pipeline.addLast("decoder", new StringDecoder())
    pipeline.addLast("handler", handler)
    pipeline
  }

  private def getFramer = {
    val delimiter = ChannelBuffers.copiedBuffer("\n".getBytes)
    new DelimiterBasedFrameDecoder(8192, true, delimiter)
  }
}
