package org.kuttz.orca.controller;

import java.util.concurrent.ExecutorService;

import org.kuttz.orca.hmon.HeartbeatMasterClient;

public interface OrcaControllerClient {
	
	public HeartbeatMasterClient getHeartbeatClient();	
	
	public ExecutorService getExecutorService();

}
