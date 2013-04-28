package org.kuttz.orca.controller;

import java.util.Map;

public class OrcaLaunchContext {
	
	private final String shellCommand;	
	private final Map<String, String> env;

	public OrcaLaunchContext(String shellCommand, Map<String, String> env) {
		super();
		this.shellCommand = shellCommand;
		this.env = env;
	}

	public String getShellCommand() {
		return shellCommand;
	}

	public Map<String, String> getEnv() {
		return env;
	}
}
