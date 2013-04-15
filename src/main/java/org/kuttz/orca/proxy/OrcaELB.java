package org.kuttz.orca.proxy;

import java.net.InetSocketAddress;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcaELB {
	
	private static Logger logger = LoggerFactory.getLogger(OrcaELB.class);
			
	private final int localPort;
	private TreeSet<ELBNode> nodeSet = new TreeSet<OrcaELB.ELBNode>();
	private ELBNode currentNode = null;

	public OrcaELB(int localPort) {
		this.localPort = localPort;
	}
	
	public int getLocalPort() {
		return localPort;
	}
	
	public OrcaELB addNode(String host, int port) {
		nodeSet.add(new ELBNode(host, port));
		return this;
	}
	
	public OrcaELB removeNode(String host, int port) {
		nodeSet.remove(new ELBNode(host, port));
		return this;
	}
	
	public synchronized ELBNode nextNode() {		
		if (currentNode == null) {
			currentNode = nodeSet.first();
		} else {
			currentNode = nodeSet.higher(currentNode);
			if (currentNode == null) {
				currentNode = nodeSet.first();
			}
		}
		return currentNode;
	}
	
	public void run() {
		logger.info("Starting OrcaELB on port[" + localPort + "]..");
		Executor cachedThreadPool = Executors.newCachedThreadPool();
		ServerBootstrap sb = 
				new ServerBootstrap(
						new NioServerSocketChannelFactory(cachedThreadPool, cachedThreadPool));
		
		NioClientSocketChannelFactory cf = 
				new NioClientSocketChannelFactory(cachedThreadPool, cachedThreadPool);
		
		sb.setPipelineFactory(new ELBPipelineFactory(cf, this));
		
		sb.bind(new InetSocketAddress(localPort));		
	}
	
	public static class ELBNode implements Comparable<ELBNode> {
		public final String host;
		public final int port;
		public ELBNode(String host, int port) {
			super();
			this.host = host;
			this.port = port;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((host == null) ? 0 : host.hashCode());
			result = prime * result + port;
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ELBNode other = (ELBNode) obj;
			if (host == null) {
				if (other.host != null)
					return false;
			} else if (!host.equals(other.host))
				return false;
			if (port != other.port)
				return false;
			return true;
		}
		@Override
		public String toString() {
			return "ELBNode [host=" + host + ", port=" + port + "]";
		}
		@Override
		public int compareTo(ELBNode o) {
			return ("" + host + port).compareTo("" + o.host + o.port);
		}		
	}
	
	
	public static void main(String[] args) {
		OrcaELB elb = new OrcaELB(8811).addNode("localhost", 8812).addNode("localhost", 8813);
		elb.run();		
	}

}
