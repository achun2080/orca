package org.kuttz.orca.web;

import com.beust.jcommander.Parameter;

public class ContainerArgs {
	
	@Parameter(names = "-app_name", description = "Application Name", required = true)
	public String appName;
	
	@Parameter(names = "-war_location", description = "Location of War File", required = true)
	public String warLocation;	

}
