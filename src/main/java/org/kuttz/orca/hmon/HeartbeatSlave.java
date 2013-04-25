package org.kuttz.orca.hmon;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.kuttz.orca.hmon.THeartbeatEndPoint.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatSlave implements THeartbeatCommandEndPoint.Iface, Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(HeartbeatSlave.class);
	
	private ScheduledExecutorService schedExService = null;
	
	private HBSlaveArgs slaveArgs;
	
	private volatile boolean hasStarted = false;
	
	private volatile int runningPort;
	
	private volatile NodeInfo nodeInfo = null;
	
	public HeartbeatSlave(HBSlaveArgs slaveArgs) {
		this.slaveArgs = slaveArgs;
	}
	
	public void init() {
		this.schedExService = new ScheduledThreadPoolExecutor(slaveArgs.numSenderThreads);
	}
	
	@Override
	public void run() {
		
		this.schedExService.scheduleAtFixedRate(new Runnable() {			
			@Override
			public void run() {
				try {
					if (nodeInfo != null) {
						TTransport transport = new TFramedTransport(new TSocket(slaveArgs.masterHost, slaveArgs.masterPort, slaveArgs.sendTimeout));
						transport.open();
						TBinaryProtocol protocol = new TBinaryProtocol(transport);
						Client master = new THeartbeatEndPoint.Client(protocol);
						HeartbeatMsg hMsg = new HeartbeatMsg();
						hMsg.setNodeId(slaveArgs.nodeId);
						hMsg.setNodeType(slaveArgs.nodeType);
						hMsg.setNodeInfo(nodeInfo);
						hMsg.setHost(InetAddress.getLocalHost().getHostName());
						hMsg.setCommandPort(runningPort);
						boolean mReturn = master.acceptHeartbeat(hMsg);
						logger.debug("Is heartbeat to master successful ? [" + mReturn + "]");
					} else {
						logger.debug("NodeInfo not set yet...");
					}
				} catch (TTransportException e) {
					logger.error("Error in Thrift Transport !!", e);
				} catch (TException e) {
					logger.error("Error in connecting to Heartbeat Master !!", e);
				} catch (UnknownHostException e) {
					logger.error("Could not obtain localhost !!", e);
				}
				
			}
		}, slaveArgs.sendPeriod, slaveArgs.sendPeriod, TimeUnit.MILLISECONDS);
		
		try {
			TNonblockingServerTransport trans = createTransport(slaveArgs.minPort, slaveArgs.maxPort);
            TNonblockingServer.Args args = new TNonblockingServer.Args(trans);
            args.transportFactory(new TFramedTransport.Factory());
            args.protocolFactory(new TBinaryProtocol.Factory());
            args.processor(new THeartbeatCommandEndPoint.Processor<HeartbeatSlave>(this));
            TNonblockingServer server = new TNonblockingServer(args);
			server.serve();
		} catch (Exception e) {
			logger.error("Could not start Heartbeat Command EndPoint", e);
		}		
	}
	
	private TNonblockingServerTransport createTransport(int minPort, int maxPort) throws IOException {
		for (int p = minPort; p <= maxPort; p++) {
			try {				
				TNonblockingServerSocket t = new TNonblockingServerSocket(p);
				logger.info("Starting Heartbeat Command End point on port [" + p + "]");
				this.hasStarted = true;
				this.runningPort = p;
				return t;
			} catch (Exception e) {
				logger.info("Could not create server on port [" + p + "] !!");
				continue;
			}
		}
		
		throw new IOException("No free ports available from [" + minPort + " to " + maxPort + "]");
	}	
	
	public NodeInfo getNodeInfo() {
		return this.nodeInfo;
	}
	
	public void setNodeInfo(NodeInfo nodeInfo) {
		this.nodeInfo = nodeInfo;
	}
	
	@Override
	public boolean changeEndPoint(String host, int port) throws TException {
		slaveArgs.masterHost = host;
		slaveArgs.masterPort = port;
		return true;
	}	
	
	public static void main(String[] args) {
//		HeartbeatSlave heartbeatSlave = new HeartbeatSlave();
//		heartbeatSlave.init(NodeType.CONTAINER, Integer.parseInt(args[0]), 1, Integer.parseInt(args[1]), Integer.parseInt(args[2]), "localhost", 8811);
//		heartbeatSlave.run();
	}

}
