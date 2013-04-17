package org.kuttz.orca.hmon;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeartbeatMaster implements THeartbeatService.Iface {
	
	private static Logger logger = LoggerFactory.getLogger(HeartbeatMaster.class);
	
	private final ConcurrentHashMap<NodeType, ConcurrentHashMap<HeartbeatNode, AtomicLong>> nodeRegistry = 
			new ConcurrentHashMap<NodeType, ConcurrentHashMap<HeartbeatNode, AtomicLong>>();
	private ScheduledExecutorService schedExService = null;
	private int checkPeriod;
	// Time after which the Master will issue a warning
	private int warnTime;
	// Time after which Master will deem the node dead
	// and will remove the node from registry
	private int deadTime;
	// Client registry
	private Set<HeartbeatMasterClient> clients = new HashSet<HeartbeatMasterClient>();
	// Server port
	private int port;
	// NumThreads for heartbeatMaster;
	private int numHbThreads;	
	
	public void init(int numCheckerThreads, int numHbThreads, int checkPeriod, int warnTime, int deadTime, int port) {
		this.schedExService = new ScheduledThreadPoolExecutor(numCheckerThreads);
		this.numHbThreads = numHbThreads;
		this.checkPeriod = checkPeriod;
		this.port = port;
		this.warnTime = warnTime;
		this.deadTime = deadTime;
	}
	
	public void registerClient(HeartbeatMasterClient client) {
		clients.add(client);
	}
	
	public void deRegisterClient(HeartbeatMasterClient client) {
		clients.remove(client);
	}	
	
	public void run() {
		// Start timer
		schedExService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				long cTime = System.currentTimeMillis();
				List<HeartbeatNode> nodesToDelete = new ArrayList<HeartbeatNode>();
				for (Entry<NodeType, ConcurrentHashMap<HeartbeatNode, AtomicLong>> e : nodeRegistry.entrySet()) {
					NodeType nType = e.getKey();					
					ConcurrentHashMap<HeartbeatNode,AtomicLong> m = e.getValue();
					for (Entry<HeartbeatNode,AtomicLong> e2 : m.entrySet()) {
						HeartbeatNode node = e2.getKey();
						long tDiff = cTime - e2.getValue().get();
						logger.debug("Found Node [" + nType + ", " + node.getId() + "]");
						if (tDiff > warnTime) {
							for (HeartbeatMasterClient client : clients) {																
								client.nodeWarn(nType, node);
								logger.debug("Node hasnt sent Heartbeats for a while [" + nType + ", " + node.getId() + "]");
								if (tDiff > deadTime) {
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
		}, checkPeriod, checkPeriod, TimeUnit.MILLISECONDS);
		
		logger.info("Starting Heartbeat Master on port [" + port + "]");
		try {
			TNonblockingServerTransport trans = new TNonblockingServerSocket(port);
            THsHaServer.Args args = new THsHaServer.Args(trans);
            args.transportFactory(new TFramedTransport.Factory());
            args.protocolFactory(new TBinaryProtocol.Factory());
            args.processor(new THeartbeatService.Processor<HeartbeatMaster>(this));
            args.workerThreads(numHbThreads);
			THsHaServer server = new THsHaServer(args);
			server.serve();
		} catch (Exception e) {
			logger.error("Could not start Heartbeat Master", e);
		}		
		
	}
	
	@Override
	public boolean acceptHeartbeat(HeartbeatMsg hbMsg) throws TException {
		NodeType nodeType = hbMsg.getNodeType();
		int nodeId = hbMsg.getNodeId();
		logger.debug("Received Heartbeat from node [" + nodeType + ", " + nodeId + "]");
		ConcurrentHashMap<HeartbeatNode,AtomicLong> mTemp = new ConcurrentHashMap<HeartbeatNode, AtomicLong>();
		ConcurrentHashMap<HeartbeatNode,AtomicLong> m = nodeRegistry.putIfAbsent(nodeType, mTemp);		
		if (m == null) {
			m = mTemp;
		}
		long cTime = System.currentTimeMillis();		
		AtomicLong aTemp = new AtomicLong(cTime);
		AtomicLong ts = m.putIfAbsent(new HeartbeatNode(nodeId, nodeType), aTemp);
		if (ts != null) {
			ts.set(System.currentTimeMillis());
		}						
		return true;
	}
	
	public static void main(String[] args) {
		HeartbeatMaster heartbeatMaster = new HeartbeatMaster();
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
		heartbeatMaster.init(1, 2, Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
		heartbeatMaster.run();
	}
	
}
