package org.kuttz.orca.proxy;

import java.net.InetSocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.kuttz.orca.proxy.OrcaELB.ELBNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ELBInboundHandler extends SimpleChannelUpstreamHandler {
	
	private static Logger logger = LoggerFactory.getLogger(ELBInboundHandler.class);
	
	private final ClientSocketChannelFactory cf;
	private final OrcaELB elb;
	
	private volatile Channel outboundChannel;
	
    // This lock guards against the race condition that overrides the
    // OP_READ flag incorrectly.
    // See the related discussion: http://markmail.org/message/x7jc6mqx6ripynqf
    final Object trafficLock = new Object();	
	
	public ELBInboundHandler(ClientSocketChannelFactory cf, OrcaELB elb) {
		this.cf = cf;
		this.elb = elb;
	}

	@Override
	public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {

		final Channel inboundChannel = e.getChannel();
		inboundChannel.setReadable(false);
		
		ClientBootstrap cb = new ClientBootstrap(cf);
		cb.getPipeline().addLast("handler", new OutboundHandler(e.getChannel()));
		ELBNode nextNode = elb.nextNode();
		logger.info("\nRouting to [" + nextNode + "]\n");
		ChannelFuture f = cb.connect(new InetSocketAddress(nextNode.host, nextNode.port));
		outboundChannel = f.getChannel();
		
		f.addListener(new ChannelFutureListener() {			
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					inboundChannel.setReadable(true);
				} else {
					inboundChannel.close();
				}				
			}
		});
	}
	
	

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		ChannelBuffer msg = (ChannelBuffer) e.getMessage();
		synchronized (trafficLock) {
			outboundChannel.write(msg);
	        // If outboundChannel is saturated, do not read until notified in
            // OutboundHandler.channelInterestChanged().			
			if (!outboundChannel.isWritable()) {
				e.getChannel().setReadable(false);
			}
		}				
	}
			
	
	@Override
	public void channelInterestChanged(ChannelHandlerContext ctx,
			ChannelStateEvent e) throws Exception {
        // If inboundChannel is not saturated anymore, continue accepting
        // the incoming traffic from the outboundChannel.		
		synchronized (trafficLock) {
			if (e.getChannel().isWritable()) {
				if (outboundChannel != null) {
					outboundChannel.setReadable(true);
				}
			}
		}
	}

	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		if (outboundChannel != null) {
			closeOnFlush(outboundChannel);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		logger.error("Got Exception..", e.getCause());
		closeOnFlush(e.getChannel());
	}


	private class OutboundHandler extends SimpleChannelUpstreamHandler {

		private final Channel inboundChannel;
		
		public OutboundHandler(Channel channel) {
			this.inboundChannel = channel;
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
            ChannelBuffer msg = (ChannelBuffer) e.getMessage();
            synchronized (trafficLock) {
                inboundChannel.write(msg);
                // If inboundChannel is saturated, do not read until notified in
                if (!inboundChannel.isWritable()) {
                    e.getChannel().setReadable(false);
                }
            }
		}

		@Override
		public void channelInterestChanged(ChannelHandlerContext ctx,
				ChannelStateEvent e) throws Exception {
			// If outboundChannel is not saturated anymore, continue accepting
            // the incoming traffic from the inboundChannel.
            synchronized (trafficLock) {
                if (e.getChannel().isWritable()) {
                    inboundChannel.setReadable(true);
                }
            }
		}
		
		@Override
		public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
				throws Exception {
			closeOnFlush(inboundChannel);
		}		
		
		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			logger.error("[OutboundChannel] Got Exception..", e.getCause());
			closeOnFlush(e.getChannel());
		}		
		
	}
	
	
	
	static void closeOnFlush(Channel ch) {
		if (ch.isConnected()) {
			ch.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
		}
		
	}

	
}
