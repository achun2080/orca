package org.kuttz.orca;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.kuttz.orca.hmon.HBSlaveArgs;
import org.kuttz.orca.hmon.NodeType;
import org.kuttz.orca.proxy.ELBArgs;
import org.kuttz.orca.proxy.ProxyContainer;
import org.kuttz.orca.web.ContainerArgs;
import org.kuttz.orca.web.WebContainer;

public class OrcaDaemon {

	public static SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");		
	
	public static void main(String[] args) {
		ExecutorService tp = Executors.newCachedThreadPool();
		try {
			if (args[0].contains("proxy")) {
				startProxyContainer(tp, args);
			} else {
				startWebContainer(tp, args);
			}
			while (tp.isTerminated()) {
				tp.awaitTermination(5, TimeUnit.MINUTES);
				System.out.println("[" + df.format(Calendar.getInstance().getTime()) + "] Daemon still running !!");
			}
		} catch (Exception e) {
			
		}
		
	}
	
	public static ProxyContainer startProxyContainer(ExecutorService tp, String[] args)
			throws InterruptedException {
		HBSlaveArgs proxyHbSlaveArgs = createHbSlaveArgs(args, NodeType.PROXY);
		ELBArgs elbArgs = new ELBArgs();
		Tools.parseArgs(elbArgs, args);
		ProxyContainer proxyContainer = new ProxyContainer(elbArgs, proxyHbSlaveArgs);
		proxyContainer.init();
		tp.submit(proxyContainer);
		while (!proxyContainer.isRunning()) {
			System.out.println("[" + df.format(Calendar.getInstance().getTime()) + "] Waiting for ProxyContainer to start..");
			Thread.sleep(1000);
		}
		return proxyContainer;
	}
	
	public static WebContainer startWebContainer(ExecutorService tp, String[] args)
			throws InterruptedException {
		HBSlaveArgs containerHbSlaveArgs = createHbSlaveArgs(args, NodeType.CONTAINER);
		ContainerArgs containerArgs = new ContainerArgs();
		Tools.parseArgs(containerArgs, args);
		WebContainer webContainer = new WebContainer(containerArgs, containerHbSlaveArgs);
		webContainer.init();
		tp.submit(webContainer);		
		while (!webContainer.isRunning()) {
			System.out.println("[" + df.format(Calendar.getInstance().getTime()) + "] Waiting for WebContainer to start..");
			Thread.sleep(1000);
		}
		return webContainer;
	}
	
	private static HBSlaveArgs createHbSlaveArgs(String[] args, NodeType nType) {
		HBSlaveArgs hbSlaveArgs = new HBSlaveArgs();		
		Tools.parseArgs(hbSlaveArgs, args);
		hbSlaveArgs.nodeType = nType;
		return hbSlaveArgs;
	}	

}
