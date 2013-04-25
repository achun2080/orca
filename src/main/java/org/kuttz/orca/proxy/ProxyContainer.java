package org.kuttz.orca.proxy;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.kuttz.orca.hmon.HBSlaveArgs;
import org.kuttz.orca.hmon.HeartbeatSlave;
import org.kuttz.orca.hmon.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyContainer implements Runnable, TProxyCommandEndPoint.Iface {
	
	private static Logger logger = LoggerFactory.getLogger(ProxyContainer.class);
		
	private final OrcaELB orcaELB;
	
	private final ELBArgs elbArgs;
	
	private final HeartbeatSlave hbSlave;
	
	private ExecutorService exService;
	
	private volatile boolean hasStarted = false;
	
	private volatile int runningPort;
	
	private volatile NodeInfo nodeInfo = new NodeInfo();
	
	public ProxyContainer(ELBArgs elbArgs, HBSlaveArgs hbSlaveArgs) {
		this.hbSlave = new HeartbeatSlave(hbSlaveArgs);		
		this.orcaELB = new OrcaELB(elbArgs, this);
		this.elbArgs = elbArgs;
	}

	public void init() {
		this.hbSlave.init();
		this.orcaELB.init();
		exService = Executors.newCachedThreadPool();		
	}

	@Override
	public void run() {
		exService.submit(hbSlave);
		exService.submit(orcaELB);
		
		try {
			TNonblockingServerTransport trans = createTransport(elbArgs.minPort, elbArgs.maxPort);
            TNonblockingServer.Args args = new TNonblockingServer.Args(trans);
            args.transportFactory(new TFramedTransport.Factory());
            args.protocolFactory(new TBinaryProtocol.Factory());
            args.processor(new TProxyCommandEndPoint.Processor<ProxyContainer>(this));
            TNonblockingServer server = new TNonblockingServer(args);
			server.serve();
		} catch (Exception e) {
			logger.error("Could not start ELB Command EndPoint", e);
		}		
	}
	
	private TNonblockingServerTransport createTransport(int minPort, int maxPort) throws IOException {
		for (int p = minPort; p <= maxPort; p++) {
			try {				
				TNonblockingServerSocket t = new TNonblockingServerSocket(p);
				logger.info("Starting ELB Command End point on port [" + p + "]");
				this.hasStarted = true;
				this.runningPort = p;
				setELBCommandPort();
				return t;
			} catch (Exception e) {
				logger.info("Could not create ELB command end point on port [" + p + "] !!");
				continue;
			}
		}
		
		throw new IOException("No free ports available from [" + minPort + " to " + maxPort + "]");
	}	

	// Set ELB Port in the HB msg - to be sent to master 
	public void setELBPort(int port) {
		this.nodeInfo.setAuxEndPointPort1(port);
		if (this.nodeInfo.isSetAuxEndPointPort1() && this.nodeInfo.isSetAuxEndPointPort2()) {
			hbSlave.setNodeInfo(this.nodeInfo);
		}
	}
	
	// Set port to access ELB Command End Point in Hb msg - to be sent to master 
	public void setELBCommandPort() {
		this.nodeInfo.setAuxEndPointPort2(runningPort);
		if (this.nodeInfo.isSetAuxEndPointPort1() && this.nodeInfo.isSetAuxEndPointPort2()) {
			hbSlave.setNodeInfo(this.nodeInfo);
		}
	}	

	@Override
	public boolean addNode(String host, int port, int weight) throws TException {
		this.orcaELB.addNode(host, port);
		logger.info("Added node [" + host + ", " + port + "] to ELB..");
		return true;
	}

	@Override
	public boolean removeNode(String host, int port) throws TException {
		this.orcaELB.removeNode(host, port);
		logger.info("Removed node [" + host + ", " + port + "] to ELB..");
		return true;
	}
	
	public boolean isRunning() {
		return this.hasStarted;
	}
	
	public int getRunningPort() {
		return this.runningPort;
	}	
}
