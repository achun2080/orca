package org.kuttz.orca.controller;

import com.beust.jcommander.Parameter;

public class OrcaControllerArgs {

	@Parameter(names = "-num_containers", description = "YARN parameter: Number of containers on which the Orca needs to be hosted", required = true)
	public int numContainers = 3;

	@Parameter(names = "-hb_period", description = "Heartbeat period", required = true)
	public int hbPeriod = 10000;
	
	@Parameter(names = "-hb_warn_time", description = "Time since last heartbeat for issuing a warning to client", required = true)
	public int hbWarnTime = 15000;
	
	@Parameter(names = "-hb_dead_time", description = "Time since last heartbeat for issuing a node dead msg to client", required = true)
	public int hbDeadTime = 30000;	
	
	@Parameter(names = "-hb_num_check_threads", description = "Number of threads allocated to checking the status of all nodes", required = true)
	public int hbCheckerThreads = 1;
	
	@Parameter(names = "-hb_num_master_threads", description = "Number of threads for the Heartbeat master", required = true)
	public int hbMasterThreads = 2;
	
	@Parameter(names = "-hb_min_port", description = "Min port on which HB master would listen on", required = true)
	public int hbMinPort = 8100;
	
	@Parameter(names = "-hb_max_port", description = "Miax port on which HB master would listen on", required = true)
	public int hbMaxPort = 8200;
	
	@Parameter(names = "-elb_min_port", description = "Min port on which ELB would listen on", required = true)
	public int elbMinPort = 8100;
	
	@Parameter(names = "-elb_max_port", description = "Max port on which ELB would listen on", required = true)
	public int elbMaxPort = 8200;
	
	@Parameter(names = "-app_name", description = "Application Name", required = true)
	public String appName;
	
	@Parameter(names = "-war_location", description = "Location of .war file", required = true)
	public String warLocation;	
}
