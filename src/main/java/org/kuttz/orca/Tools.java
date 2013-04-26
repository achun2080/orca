package org.kuttz.orca;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Sets;

public class Tools {

	public static JCommander parseArgs(Object jcArgs, String[] cliArgs) {
	    JCommander jc = new JCommander(jcArgs);
	    List<ParameterDescription> parameters = jc.getParameters();
	    Set<String> validParams = new HashSet<String>();
	    for (ParameterDescription pd : parameters) {
	    	validParams.add(pd.getLongestName());
	    }	    
	    ArrayList<String> tmpArgs = new ArrayList<String>();	    
	    for (int i = 0; i < cliArgs.length; i+=2) {
	    	if (validParams.contains(cliArgs[i])) {
	    		tmpArgs.add(cliArgs[i]);
	    		tmpArgs.add(cliArgs[i + 1]);
	    	}
	    }
	    String[] cliArgs2 = tmpArgs.toArray(new String[0]);	    	    
	    try {
	        if (Sets.newHashSet(cliArgs).contains("-help")) {
	    	    Parameters parametersAnnotation = jcArgs.getClass().getAnnotation(Parameters.class);
	            jc.addCommand(parametersAnnotation.commandNames()[0], jcArgs);
	            jc.usage(parametersAnnotation.commandNames()[0]);
	            System.exit(0);
	        }
	        jc.parseWithoutValidation(cliArgs2);
	    } catch (Exception e) {
	        JCommander.getConsole().println("Cannot parse arguments: " + e.getClass() + " -> " + e.getMessage());
	        jc.usage();
	        System.exit(1);
	    }
	    return jc;
	}	
}
