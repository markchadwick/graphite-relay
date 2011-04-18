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

  private val delimiters = Delimiters.lineDelimiter()

  def getPipeline = {
    val pipeline = Channels.pipeline()

    pipeline.addLast("framer",  new DelimiterBasedFrameDecoder(
                                      8192, true, delimiters:_*))

    pipeline.addLast("decoder", stringDecoder)
    pipeline.addLast("handler", handler)
    pipeline
  }

  private def getFramer = {
    new DelimiterBasedFrameDecoder(8192, true, delimiters:_*)
  }

  private val stringDecoder = new StringDecoder()
}
