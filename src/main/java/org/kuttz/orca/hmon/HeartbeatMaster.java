package org.kuttz.orca.hmon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.kuttz.orca.hmon.HeartbeatNode.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatMaster implements THeartbeatEndPoint.Iface, Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(HeartbeatMaster.class);
	
	private final ConcurrentHashMap<NodeType, ConcurrentHashMap<HeartbeatNode, NodeState>> nodeRegistry = 
			new ConcurrentHashMap<NodeType, ConcurrentHashMap<HeartbeatNode, NodeState>>();
	private ScheduledExecutorService schedExService = null;

	private Set<HeartbeatMasterClient> clients = new HashSet<HeartbeatMasterClient>();
	
	private HBMasterArgs masterArgs;
	
	private volatile boolean hasStarted = false;
	
	private volatile int runningPort;
	
	public HeartbeatMaster(HBMasterArgs args) {
		this.masterArgs = args;
	}	
	
	public void registerClient(HeartbeatMasterClient client) {
		clients.add(client);
	}
	
	public void deRegisterClient(HeartbeatMasterClient client) {
		clients.remove(client);
	}	
	
	public void init() {
		this.schedExService = new ScheduledThreadPoolExecutor(masterArgs.numCheckerThreads);
	}
	
	@Override
	public void run() {
		// Start timer
		schedExService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				long cTime = System.currentTimeMillis();
				List<HeartbeatNode> nodesToDelete = new ArrayList<HeartbeatNode>();
				for (Entry<NodeType, ConcurrentHashMap<HeartbeatNode, NodeState>> e : nodeRegistry.entrySet()) {
					NodeType nType = e.getKey();					
					ConcurrentHashMap<HeartbeatNode,NodeState> m = e.getValue();
					for (Entry<HeartbeatNode,NodeState> e2 : m.entrySet()) {
						HeartbeatNode node = e2.getKey();
						long tDiff = cTime - e2.getValue().timeStamp.get();
						logger.debug("Found Node [" + nType + ", " + node.getId() + "]");
						if (tDiff > masterArgs.warnTime) {
							for (HeartbeatMasterClient client : clients) {																
								client.nodeWarn(nType, node);
								logger.debug("Node hasnt sent Heartbeats for a while [" + nType + ", " + node.getId() + "]");
								if (tDiff > masterArgs.deadTime) {
									client.nodeDead(nType, node);
									nodesToDelete.add(node);
									logger.debug("Node deemed dead [" + nType + ", " + node.getId() + "]");
								}
							}
						}						
					}
				}
				for (HeartbeatNode node : nodesToDelete) {
					nodeRegistry.get(node.getType()).remove(node);
				}
				
			}			
		}, masterArgs.checkPeriod, masterArgs.checkPeriod, TimeUnit.MILLISECONDS);
		
		try {
			TNonblockingServerTransport trans = createTransport(masterArgs.minPort, masterArgs.maxPort);
            THsHaServer.Args args = new THsHaServer.Args(trans);
            args.transportFactory(new TFramedTransport.Factory());
            args.protocolFactory(new TBinaryProtocol.Factory());
            args.processor(new THeartbeatEndPoint.Processor<HeartbeatMaster>(this));
            args.workerThreads(masterArgs.numHbThreads);
			THsHaServer server = new THsHaServer(args);
			server.serve();
		} catch (Exception e) {
			logger.error("Could not start Heartbeat Master", e);
		}		
		
	}
		
	private TNonblockingServerTransport createTransport(int minPort, int maxPort) throws IOException {
		for (int p = minPort; p <= maxPort; p++) {
			try {				
				TNonblockingServerSocket t = new TNonblockingServerSocket(p);
				logger.info("Starting Heartbeat Master on port [" + p + "]");
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
	
	@Override
	public boolean acceptHeartbeat(HeartbeatMsg hbMsg) throws TException {
		NodeType nodeType = hbMsg.getNodeType();
		int nodeId = hbMsg.getNodeId();
		logger.debug("Received Heartbeat from node [" + nodeType + ", " + nodeId + "]");
		ConcurrentHashMap<HeartbeatNode,NodeState> mTemp = new ConcurrentHashMap<HeartbeatNode, NodeState>();
		ConcurrentHashMap<HeartbeatNode,NodeState> m = nodeRegistry.putIfAbsent(nodeType, mTemp);		
		if (m == null) {
			m = mTemp;
		}
		long cTime = System.currentTimeMillis();		
		NodeState ns = new NodeState();
		ns.timeStamp.set(cTime);
		ns.host = hbMsg.getHost();
		ns.port = hbMsg.getCommandPort();		
		NodeState ns1 = m.putIfAbsent(new HeartbeatNode(nodeId, nodeType), ns);
		if (ns1 != null) {
			ns1.timeStamp.set(cTime);
			ns1.host = hbMsg.getHost();
			ns1.port = hbMsg.getCommandPort();
		}						
		return true;
	}
	
	public boolean isRunning() {
		return this.hasStarted;
	}
	
	public int getRunningPort() {
		return this.runningPort;
	}
	
	public static void main(String[] args) throws Exception {
		HBMasterArgs hbArgs = new HBMasterArgs();
		hbArgs.checkPeriod = 10000;
		hbArgs.deadTime = 30000;
		hbArgs.numCheckerThreads = 1;
		hbArgs.numHbThreads = 2;
		hbArgs.minPort = 8810;
		hbArgs.maxPort = 8850;
		hbArgs.warnTime = 15000;
		
		
		HeartbeatMaster heartbeatMaster = new HeartbeatMaster(hbArgs);
		heartbeatMaster.registerClient(new HeartbeatMasterClient() {
			@Override
			public void nodeWarn(NodeType nType, HeartbeatNode node) {
				System.out.println("Client warned of node inactivity !!");
			}
			@Override
			public void nodeDead(NodeType nType, HeartbeatNode node) {
				System.out.println("Client notified of node death !!");
			}
		});
		heartbeatMaster.init();
		
		System.out.println("Has started 1 [" + heartbeatMaster.isRunning() + ", " + heartbeatMaster.getRunningPort() + "]");		
		new Thread(heartbeatMaster).start();
		Thread.sleep(2000);
		System.out.println("Has started 2 [" + heartbeatMaster.isRunning() + ", " + heartbeatMaster.getRunningPort() + "]");
		
		
	}
	
}
