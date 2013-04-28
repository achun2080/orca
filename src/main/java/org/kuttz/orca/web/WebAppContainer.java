package org.kuttz.orca.web;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.kuttz.orca.hmon.HBSlaveArgs;
import org.kuttz.orca.hmon.HeartbeatSlave;
import org.kuttz.orca.hmon.NodeInfo;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebAppContainer implements Runnable {
	
	private static Logger logger = LoggerFactory.getLogger(WebAppContainer.class);
	
	private final HeartbeatSlave hbSlave;
	
	private Server jettyServer;
	
	private final WebAppArgs containerArgs;
	
	private ExecutorService exService;
	
	private volatile boolean hasStarted = false;
	
	private volatile int runningPort;
	
	public WebAppContainer(WebAppArgs containerArgs, HBSlaveArgs hbSlaveArgs) {
		this.hbSlave = new HeartbeatSlave(hbSlaveArgs);
		this.containerArgs = containerArgs;
	}
	
	public void init() {
		this.hbSlave.init();
		exService = Executors.newCachedThreadPool();
	}

	@Override
	public void run() {
		exService.submit(hbSlave);
		WebAppContext webApp = new WebAppContext();
		webApp.setContextPath("/" + containerArgs.appName);
		webApp.setWar(containerArgs.warLocation);
		
		// Let Jetty start on any available port
		this.jettyServer = new Server(0);
		this.jettyServer.setHandler(webApp);
		try {
			this.jettyServer.start();
			this.hasStarted = true;
			this.runningPort = (this.jettyServer.getConnectors()[0]).getLocalPort();
			logger.info("Starting Web Container on port [" + this.runningPort + "]");
		} catch (Exception e) {
			logger.error("Could not start Web Container !!", e);
			System.exit(-1);
		}
		
		try {
			NodeInfo nodeInfo = new NodeInfo();
			nodeInfo.setAuxEndPointPort1(runningPort);
			hbSlave.setNodeInfo(nodeInfo);
			this.jettyServer.join();
		} catch (InterruptedException e) {
			logger.error("Web Container interrupted!!", e);
		}
		
	}
	
	public boolean isRunning() {
		return this.hasStarted;
	}
	
	public int getRunningPort() {
		return this.runningPort;
	}	
}
