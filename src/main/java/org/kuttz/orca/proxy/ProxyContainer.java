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
	
	public void setELBPort(int port) {
		hbSlave.getNodeInfo().setAuxEndPointPort1(port);
	}
	
	public void setELBCommandPort() {
		hbSlave.getNodeInfo().setAuxEndPointPort2(runningPort);
	}	

	@Override
	public boolean addNode(String host, int port, int weight) throws TException {
		this.orcaELB.addNode(host, port);
		return true;
	}

	@Override
	public boolean removeNode(String host, int port) throws TException {
		this.orcaELB.removeNode(host, port);
		return true;
	}
}
