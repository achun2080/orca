package org.kuttz.orca;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;

public class Tools {

	public static JCommander parseArgs(Object jcArgs, String[] cliArgs) {
	    JCommander jc = new JCommander(jcArgs);
	    try {
	        if (Sets.newHashSet(cliArgs).contains("-help")) {
	            Parameters parametersAnnotation = jcArgs.getClass().getAnnotation(Parameters.class);
	            jc.addCommand(parametersAnnotation.commandNames()[0], jcArgs);
	            jc.usage(parametersAnnotation.commandNames()[0]);
	            System.exit(0);
	        }
	        jc.parse(cliArgs);
	    } catch (Exception e) {
	        JCommander.getConsole().println("Cannot parse arguments: " + e.getClass() + " -> " + e.getMessage());
	        jc.usage();
	        System.exit(1);
	    }
	    return jc;
	}	
}
