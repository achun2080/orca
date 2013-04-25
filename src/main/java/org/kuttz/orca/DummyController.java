package org.kuttz.orca;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.kuttz.orca.hmon.HBMasterArgs;
import org.kuttz.orca.hmon.HBSlaveArgs;
import org.kuttz.orca.hmon.HeartbeatMaster;
import org.kuttz.orca.hmon.HeartbeatNode.NodeState;
import org.kuttz.orca.hmon.NodeType;
import org.kuttz.orca.proxy.ELBArgs;
import org.kuttz.orca.proxy.ProxyContainer;
import org.kuttz.orca.proxy.TProxyCommandEndPoint;
import org.kuttz.orca.proxy.TProxyCommandEndPoint.Client;
import org.kuttz.orca.web.ContainerArgs;
import org.kuttz.orca.web.WebContainer;

public class DummyController {
	
	public static void main(String[] args) throws Exception {
		
		ExecutorService tp = Executors.newCachedThreadPool();		
		
		HeartbeatMaster hbMaster = startHbMaster(tp);
		int hbMasterPort = hbMaster.getRunningPort();
		
		int proxyNodeId = 1;
		startProxyContainer(tp, proxyNodeId, hbMasterPort);		
		ELBInfo elbInfo = waitForELBInfo(hbMaster, proxyNodeId);
		
		int webNodeId1 = 2;
		startWebContainer(tp, webNodeId1, hbMasterPort);
		ContainerInfo containerInfo1 = waitForContainerInfo(hbMaster, webNodeId1);
		
		int webNodeId2 = 3;
		startWebContainer(tp, webNodeId2, hbMasterPort);
		ContainerInfo containerInfo2 = waitForContainerInfo(hbMaster, webNodeId2);
		
		TTransport transport = new TFramedTransport(new TSocket(elbInfo.elbHost, elbInfo.elbCommandPort, 5000));
		transport.open();
		TBinaryProtocol protocol = new TBinaryProtocol(transport);
		Client elbProxy = new TProxyCommandEndPoint.Client(protocol);
		elbProxy.addNode(containerInfo1.containerHost, containerInfo1.containerPort, -1);
		elbProxy.addNode(containerInfo2.containerHost, containerInfo2.containerPort, -1);
		
	}

	private static WebContainer startWebContainer(ExecutorService tp, int webNodeId, int hbMasterPort)
			throws InterruptedException {
		HBSlaveArgs containerHbSlaveArgs = createHbSlaveArgs(NodeType.CONTAINER, webNodeId, hbMasterPort);
		ContainerArgs containerArgs = new ContainerArgs();
		containerArgs.appName = "canary";
		containerArgs.warLocation = "/Users/suresa1/tomcat_home/webapps/gphdmgr-api.war";
		WebContainer webContainer = new WebContainer(containerArgs, containerHbSlaveArgs);
		webContainer.init();
		tp.submit(webContainer);		
		while (!webContainer.isRunning()) {
			System.out.println("Waiting for WebContainer to start..");
			Thread.sleep(1000);
		}
		return webContainer;
	}
	
	private static ContainerInfo waitForContainerInfo(HeartbeatMaster hbMaster, int webNodeId) throws InterruptedException {
		ContainerInfo cInfo = new ContainerInfo();
		NodeState nodeState = null;
		while (nodeState == null) {
			System.out.println("Waiting for WebContainer HbSlave to send HbMsg..");
			Thread.sleep(1000);
			nodeState = hbMaster.getNodeState(webNodeId, NodeType.CONTAINER);
			if (nodeState == null) continue;
			if (!nodeState.nodeInfo.isSetAuxEndPointPort1()) {
				System.out.println("Jetty port not set yet !!");
				nodeState = null;
				continue;
			}
			cInfo.containerPort = nodeState.nodeInfo.getAuxEndPointPort1();
			cInfo.containerHost = nodeState.host;
			System.out.println("WebContainer Info :         [" + cInfo + "]");
		}
		return cInfo;
	}	

	private static ProxyContainer startProxyContainer(ExecutorService tp, int nodeId, int hbMasterPort)
			throws InterruptedException {
		HBSlaveArgs proxyHbSlaveArgs = createHbSlaveArgs(NodeType.PROXY, nodeId, hbMasterPort);
		ELBArgs elbArgs = new ELBArgs();
		elbArgs.minPort = 8100;
		elbArgs.maxPort = 8200;
		ProxyContainer proxyContainer = new ProxyContainer(elbArgs, proxyHbSlaveArgs);
		proxyContainer.init();
		tp.submit(proxyContainer);
		while (!proxyContainer.isRunning()) {
			System.out.println("Waiting for ProxyContainer to start..");
			Thread.sleep(1000);
		}
		return proxyContainer;
	}	
	
	private static ELBInfo waitForELBInfo(HeartbeatMaster hbMaster, int proxyNodeId) throws InterruptedException {
		ELBInfo elbInfo = new ELBInfo();
		NodeState nodeState = null;
		while (nodeState == null) {
			System.out.println("Waiting for ProxyContainer HbSlave to send HbMsg..");
			Thread.sleep(1000);
			nodeState = hbMaster.getNodeState(proxyNodeId, NodeType.PROXY);
			if (nodeState == null) continue;
			if (!nodeState.nodeInfo.isSetAuxEndPointPort1()) {
				System.out.println("ELB port not set yet !!");
				nodeState = null;
				continue;
			}
			elbInfo.elbPort = nodeState.nodeInfo.getAuxEndPointPort1();
			if (!nodeState.nodeInfo.isSetAuxEndPointPort2()) {
				System.out.println("ELB Command EndPoint port not set yet !!");
				nodeState = null;
				continue;
			}
			elbInfo.elbCommandPort = nodeState.nodeInfo.getAuxEndPointPort2();
			elbInfo.elbHost = nodeState.host;
			System.out.println("Elb Info :         [" + elbInfo + "]");
		}
		return elbInfo;
	}
	
	private static HeartbeatMaster startHbMaster(ExecutorService tp) throws InterruptedException {
		HBMasterArgs hbMasterArgs = new HBMasterArgs();
		hbMasterArgs.checkPeriod = 10000;
		hbMasterArgs.deadTime = 30000;
		hbMasterArgs.warnTime = 30000;
		hbMasterArgs.minPort = 8100;
		hbMasterArgs.maxPort = 8200;
		hbMasterArgs.numCheckerThreads = 1;
		hbMasterArgs.numHbThreads = 2;
		
		HeartbeatMaster hbMaster = new HeartbeatMaster(hbMasterArgs);
		hbMaster.init();
		tp.submit(hbMaster);
		while (!hbMaster.isRunning()) {
			System.out.println("Waiting for HBMaster to start..");
			Thread.sleep(1000);
		}
		return hbMaster;
	}
	
	private static HBSlaveArgs createHbSlaveArgs(NodeType nType, int nodeId, int hbMasterPort) {
		HBSlaveArgs hbSlaveArgs = new HBSlaveArgs();
		hbSlaveArgs.masterHost = "localhost";
		hbSlaveArgs.masterPort = hbMasterPort;
		hbSlaveArgs.minPort = 8100;
		hbSlaveArgs.maxPort = 8200;
		hbSlaveArgs.nodeId = nodeId;
		hbSlaveArgs.nodeType = nType;
		hbSlaveArgs.numSenderThreads = 1;
		hbSlaveArgs.sendPeriod = 10000;
		hbSlaveArgs.sendTimeout = 5000;
		return hbSlaveArgs;
	}
	
	private static class ELBInfo {
		public int elbPort = -1;
		public int elbCommandPort = -1;
		public String elbHost = null;		
		@Override
		public String toString() {
			return "ELBInfo [elbPort=" + elbPort + ", elbCommandPort="
					+ elbCommandPort + ", elbHost=" + elbHost + "]";
		}
	}
	
	private static class ContainerInfo {
		public int containerPort = -1;
		public String containerHost = null;
		@Override
		public String toString() {
			return "ContainerInfo [containerPort=" + containerPort
					+ ", containerHost=" + containerHost + "]";
		}		
	}

}
