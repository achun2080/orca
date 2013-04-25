package org.kuttz.orca.hmon;

import com.beust.jcommander.Parameter;

public class HBSlaveArgs {
		
	@Parameter(names = "-send_period", description = "Time period for sending Heartbeats", required = true)
	public int sendPeriod;
	
	@Parameter(names = "-master_host", description = "Heartbeat Master hostname", required = true)
	public String masterHost;
	
	@Parameter(names = "-master_port", description = "Heartbeat Master port", required = true)
	public int masterPort;
	
	@Parameter(names = "-send_timeout", description = "Heartbeat send timeout", required = true)
	public int sendTimeout;	
	
	@Parameter(names = "-node_id", description = "Node Id", required = true)
	public int nodeId;
	
	@Parameter(names = "-num_sender_threads", description = "Node Id", required = false)
	public int numSenderThreads = 1;

	@Parameter(names = "-min_port", description = "Heartbeat Command Endpoint minPort", required = true)
	public int minPort;

	@Parameter(names = "-max_port", description = "Heartbeat Command Endpoint maxPort", required = true)
	public int maxPort;	
	
	public NodeType nodeType;
}
