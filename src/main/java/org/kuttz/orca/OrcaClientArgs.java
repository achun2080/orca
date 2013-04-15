package org.kuttz.orca;

import com.beust.jcommander.Parameter;

public class OrcaClientArgs {

	@Parameter(names = "-container_memory", description = "YARN container memory", required = false)
	int containerMemory = 256;	
	
	@Parameter(names = "-priority", description = "YARN parameter: Application priority", required = false)
	int priority = 0;

	@Parameter(names = "-user", description = "YARN parameter: User to run the application as", required = false)
	String user = "";
	
	@Parameter(names = "-num_containers", description = "YARN parameter: Number of containers on which the Orca needs to be hosted", required = true)
	int numContainers = 1;
	
	@Parameter(names = "-orca_dir", description = "YARN parameter: directory containing orca jar and webapp war", required = true)
	String orcaDir;	
	
	@Parameter(names = "-debug", description = "YARN parameter: Dump out debug information", required = false)
	boolean debug = false;	
	
}
