package org.kuttz.orca;

import com.beust.jcommander.Parameter;

public class OrcaAMArgs {

	@Parameter(names = "-num_containers", description = "YARN parameter: Number of containers on which the Orca needs to be hosted", required = true)
	int numContainers = 1;	
}
