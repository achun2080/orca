package org.kuttz.orca;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.kuttz.orca.controller.OrcaController;
import org.kuttz.orca.controller.OrcaController.ControllerRequest;
import org.kuttz.orca.controller.OrcaController.ControllerRequest.ReqType;
import org.kuttz.orca.controller.OrcaController.Node;
import org.kuttz.orca.controller.OrcaControllerArgs;
import org.kuttz.orca.controller.OrcaControllerClient;
import org.kuttz.orca.controller.OrcaLaunchContext;
import org.kuttz.orca.hmon.HeartbeatMasterClient;
import org.kuttz.orca.hmon.HeartbeatNode;
import org.kuttz.orca.hmon.HeartbeatNode.NodeState;

public class DummyClient implements OrcaControllerClient, HeartbeatMasterClient {	

	private ExecutorService tp = Executors.newCachedThreadPool();	
	
	@Override
	public HeartbeatMasterClient getHeartbeatClient() {
		return this;
	}

	@Override
	public ExecutorService getExecutorService() {
		return this.tp;
	}

	@Override
	public void nodeUp(HeartbeatNode node, NodeState firstNodeState) {
		System.out.println("Got NodeUp [" + node + ", " + firstNodeState + "]");
	}

	@Override
	public void nodeWarn(HeartbeatNode node, NodeState lastNodeState) {
		System.out.println("Got NodeWarn [" + node + ", " + lastNodeState + "]");		
	}

	@Override
	public void nodeDead(HeartbeatNode node, NodeState lastNodeState) {
		System.out.println("Got NodeDead [" + node + ", " + lastNodeState + "]");		
	}
	
	private static AtomicInteger nodeIds = new AtomicInteger(1);
	
	public static void main(String[] args) throws InterruptedException {
		OrcaControllerArgs ocArgs = new OrcaControllerArgs();		
		Tools.parseArgs(ocArgs, args);
		
		DummyClient client = new DummyClient();
		OrcaController oc = new OrcaController(ocArgs, client);
		oc.init();
		client.tp.submit(oc);
		
		boolean printPort = false;
		
		while(true) {
			ControllerRequest req = oc.getNextRequest();
			System.out.println("Got Request [" + req.getType() + "]" );
			if (req.getType().equals(ReqType.CONTAINER)) {
				final int nId = nodeIds.incrementAndGet();
				Node node = new OrcaController.Node() {					
					@Override
					public int getId() {
						return nId;
					}
				};
				req.setResponse(node);
			} else {
				OrcaLaunchContext launchContext = req.getLaunchContext();
				System.out.println("Current Classpath : " + System.getProperty("java.class.path"));
				try {
					System.out.println("Executing Command : " + launchContext.getShellCommand());
					String cp = System.getProperty("java.class.path") + ":" + "/Users/suresa1/stuff/java/orca_install/orca-1.0.0.jar";
					System.out.println("Classpath : " + cp);
					Process cmdProc = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", launchContext.getShellCommand()}, 
							new String[] {"CLASSPATH=" + cp});
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (!printPort) {
				int elbPort = oc.getProxyPort();
				if (elbPort > -1) {
					printPort = true;
				}
				System.out.println("\n ELB PORT : [" + elbPort + "]\n");
			}
		}
		
	}	

}
