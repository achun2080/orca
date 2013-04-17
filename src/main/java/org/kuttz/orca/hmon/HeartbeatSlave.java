package org.kuttz.orca.hmon;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.kuttz.orca.hmon.THeartbeatService.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatSlave {
	
	private static Logger logger = LoggerFactory.getLogger(HeartbeatSlave.class);
	
	private ScheduledExecutorService schedExService = null;
	
	private int sendPeriod;
	
	private String masterHost;
	
	private int masterPort;
	
	private int sendTimeout;
	
	private NodeType nodeType;
	
	private int nodeId;
	
	private volatile NodeMetrics nodeMetrics = new NodeMetrics();
	
	public void init(
			NodeType nodeType,
			int nodeId,
			int numSenderThreads, 
			int sendPeriod, 
			int sendTimeout, 
			String masterHost, 
			int masterPort) {
		this.schedExService = new ScheduledThreadPoolExecutor(numSenderThreads);
		this.sendPeriod = sendPeriod;
		this.masterHost = masterHost;
		this.masterPort = masterPort;
		this.sendTimeout = sendTimeout;
		this.nodeType = nodeType;
		this.nodeId = nodeId;
	}
	
	public void run() {
		this.schedExService.scheduleAtFixedRate(new Runnable() {			
			@Override
			public void run() {
				TTransport transport = new TFramedTransport(new TSocket(masterHost, masterPort, sendTimeout));
				try {
					transport.open();
					TBinaryProtocol protocol = new TBinaryProtocol(transport);
					Client master = new THeartbeatService.Client(protocol);
					HeartbeatMsg hMsg = new HeartbeatMsg();
					hMsg.setNodeId(nodeId);
					hMsg.setNodeType(nodeType);
					hMsg.setNodeMetrics(nodeMetrics);
					boolean mReturn = master.acceptHeartbeat(hMsg);
					logger.debug("Successfully sent heartbeat to master [" + mReturn + "]");
				} catch (TTransportException e) {
					logger.error("Error in Thrift Transport !!", e);
				} catch (TException e) {
					logger.error("Error in connecting to Heartbeat Master !!", e);
				}
				
			}
		}, this.sendPeriod, this.sendPeriod, TimeUnit.MILLISECONDS);
	}
	
	public void setNodeMetrics(NodeMetrics nodeMetrics) {
		this.nodeMetrics = nodeMetrics;
	}
	
	public static void main(String[] args) {
		HeartbeatSlave heartbeatSlave = new HeartbeatSlave();
		heartbeatSlave.init(NodeType.CONTAINER, Integer.parseInt(args[0]), 1, Integer.parseInt(args[1]), Integer.parseInt(args[2]), "localhost", 8811);
		heartbeatSlave.run();
	}

}
