package org.kuttz.orca.hmon;

public class HBMasterArgs {
	
	public int checkPeriod;
	// Time after which the Master will issue a warning
	public int warnTime;
	// Time after which Master will deem the node dead
	// and will remove the node from registry
	public int deadTime;
	// Server minPort
	public int minPort;
	// Server maxPort
	public int maxPort;	
	// NumThreads for heartbeatMaster;
	public int numHbThreads;
	
	public int numCheckerThreads;

}
