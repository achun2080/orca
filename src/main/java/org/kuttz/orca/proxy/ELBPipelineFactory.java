package org.kuttz.orca.proxy;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;

import static org.jboss.netty.channel.Channels.*;

public class ELBPipelineFactory implements ChannelPipelineFactory {
	
	private ClientSocketChannelFactory cf;
	private ELB elb;

	public ELBPipelineFactory(ClientSocketChannelFactory cf, ELB elb) {
		this.cf = cf;
		this.elb = elb;
	}

	@Override
	public ChannelPipeline getPipeline() throws Exception {
		ChannelPipeline cp = pipeline();
		cp.addLast("handler", new ELBInboundHandler(cf, elb));
		return cp;
	}		
}
