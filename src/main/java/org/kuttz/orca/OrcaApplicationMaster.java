package org.kuttz.orca;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.kuttz.orca.controller.OrcaControllerArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcaApplicationMaster {

	private static Logger logger = LoggerFactory.getLogger(OrcaApplicationMaster.class);
	
	private final OrcaControllerArgs orcaArgs;
	
	// Configuration
	private Configuration conf;

	// YARN RPC to communicate with the Resource Manager or Node Manager
	private YarnRPC rpc;

	// Handle to communicate with the Resource Manager
	private AMRMProtocol resourceManager;

	// Application Attempt Id ( combination of attemptId and fail count )
	private ApplicationAttemptId appAttemptID;

	// For status update for clients - yet to be implemented
	// Hostname of the container
	private final String appMasterHostname = "";
	// Port on which the app master listens for status update requests from clients
	private final int appMasterRpcPort = 0;
	// Tracking url to which app master publishes info for clients to monitor
	private final String appMasterTrackingUrl = "";

	// Incremental counter for rpc calls to the RM
	private final AtomicInteger rmRequestID = new AtomicInteger();
	
	// Simple flag to denote whether all works is done
	private boolean appDone = false;
	// Counter for completed containers ( complete denotes successful or failed )
	private final AtomicInteger numCompletedContainers = new AtomicInteger();
	// Allocated container count so that we know how many containers has the RM
	// allocated to us
	private final AtomicInteger numAllocatedContainers = new AtomicInteger();
	// Count of failed containers
	private final AtomicInteger numFailedContainers = new AtomicInteger();
	// Count of containers already requested from the RM
	// Needed as once requested, we should not request for containers again and again.
	// Only request for more if the original requirement changes.
	private final AtomicInteger numRequestedContainers = new AtomicInteger();

	// Containers to be released
	private final CopyOnWriteArrayList<ContainerId> releasedContainers = new CopyOnWriteArrayList<ContainerId>();

	// Launch threads
	private final List<Thread> launchThreads = new ArrayList<Thread>();

	private int containerMemory;
	
	public static void main(String[] args) {
		logger.info("Starting Orca Application Master..");
		
		OrcaControllerArgs orcaArgs = new OrcaControllerArgs();
		logger.info("OrcaAM args = " + Arrays.toString(args));
		Tools.parseArgs(orcaArgs, args);
		
		OrcaApplicationMaster appMaster = new OrcaApplicationMaster(orcaArgs);
		boolean result = false;
		try {
			appMaster.init();
			result = appMaster.run();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error running Orca ApplicationMaster", e);
			System.exit(1);
		}
		if (result) {
			logger.info("Orca ApplicationMaster completed successfully..");
			System.exit(0);
		}
		logger.info("Orca ApplicationMaster failed..");
		System.exit(1);				
	}
	
	public OrcaApplicationMaster(OrcaControllerArgs orcaArgs) {
		this.orcaArgs = orcaArgs;
	}
	
	public void init() throws IOException {
		containerMemory = 256;
		
		Map<String, String> envs = System.getenv();
		
		appAttemptID = Records.newRecord(ApplicationAttemptId.class);
		if (!envs.containsKey(ApplicationConstants.AM_CONTAINER_ID_ENV)) {
			throw new IllegalArgumentException("Application Attempt Id not set in the environment");
		} else {
		    ContainerId containerId = ConverterUtils.toContainerId(envs.get(ApplicationConstants.AM_CONTAINER_ID_ENV));
		    appAttemptID = containerId.getApplicationAttemptId();
		}		
		
		logger.info("Application master for app" + ", appId=" + appAttemptID.getApplicationId().getId()
		        + ", clustertimestamp=" + appAttemptID.getApplicationId().getClusterTimestamp() + ", attemptId="
		        + appAttemptID.getAttemptId());

		conf = new YarnConfiguration();
		rpc = YarnRPC.create(conf);		
	}	
	
	public boolean run() throws YarnRemoteException {
		logger.info("Starting ApplicationMaster");

		// Connect to ResourceManager
		resourceManager = connectToRM();

		// Setup local RPC Server to accept status requests directly from clients
		// TODO need to setup a protocol for client to be able to communicate to the RPC server
		// TODO use the rpc port info to register with the RM for the client to
		// send requests to this app master

		// Register self with ResourceManager
		RegisterApplicationMasterResponse response = registerToRM();
		// Dump out information about cluster capability as seen by the resource
		// manager
		int minMem = response.getMinimumResourceCapability().getMemory();
		int maxMem = response.getMaximumResourceCapability().getMemory();
		logger.info("Min mem capability of resources in this cluster " + minMem);
		logger.info("Max mem capability of resources in this cluster " + maxMem);

		// A resource ask has to be atleast the minimum of the capability of the
		// cluster, the value has to be
		// a multiple of the min value and cannot exceed the max.
		// If it is not an exact multiple of min, the RM will allocate to the
		// nearest multiple of min
		if (containerMemory < minMem) {
			logger.info("Container memory for Orca node specified below min threshold of YARN cluster. Using min value."
					+ ", specified=" + containerMemory + ", min=" + minMem);
			containerMemory = minMem;
		} else if (containerMemory > maxMem) {
			logger.info("Container memory for Orca node specified above max threshold of YARN cluster. Using max value."
					+ ", specified=" + containerMemory + ", max=" + maxMem);
			containerMemory = maxMem;
		}

		int loopCounter = -1;

		while (numCompletedContainers.get() < orcaArgs.numContainers && !appDone) {
			loopCounter++;

			// log current state
			logger.info("Current application state: loop=" + loopCounter
					+ ", appDone=" + appDone + ", total=" + orcaArgs.numContainers
					+ ", requested=" + numRequestedContainers + ", completed="
					+ numCompletedContainers + ", failed="
					+ numFailedContainers + ", currentAllocated="
					+ numAllocatedContainers);

			// Sleep before each loop when asking RM for containers
			// to avoid flooding RM with spurious requests when it
			// need not have any available containers
			// Sleeping for 1000 ms.
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.info("Sleep interrupted " + e.getMessage());
			}
			
	        // No. of containers to request
	        // For the first loop, askCount will be equal to total containers needed
	        // From that point on, askCount will always be 0 as current implementation
	        // does not change its ask on container failures.
	        int askCount = orcaArgs.numContainers - numRequestedContainers.get();
	        numRequestedContainers.addAndGet(askCount);

	        // Setup request to be sent to RM to allocate containers
	        List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
	        if (askCount > 0) {
	            ResourceRequest containerAsk = setupContainerAskForRM(askCount);
	            resourceReq.add(containerAsk);
	        }
			
	        // Send request to RM
	        logger.info("Asking RM for containers" + ", askCount=" + askCount);
	        AMResponse amResp = sendContainerAskToRM(resourceReq);
	        
	        // Retrieve list of allocated containers from the response
	        List<Container> allocatedContainers = amResp.getAllocatedContainers();
	        logger.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
	        numAllocatedContainers.addAndGet(allocatedContainers.size());
	        for (Container allocatedContainer : allocatedContainers) {
	        	logger.info("Launching Orca node on a new container." + ", containerId=" + allocatedContainer.getId()
	        			+ ", containerNode=" + allocatedContainer.getNodeId().getHost() + ":"
	        			+ allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
	        			+ allocatedContainer.getNodeHttpAddress() + ", containerState" + allocatedContainer.getState()
	        			 + ", containerResourceMemory" + allocatedContainer.getResource().getMemory());

	            LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer);
	            Thread launchThread = new Thread(runnableLaunchContainer);

	            // launch and start the container on a separate thread to keep the main thread unblocked
	            // as all containers may not be allocated at one go.
	            launchThreads.add(launchThread);
	            launchThread.start();	        	
	        }
	        
	        // Check what the current available resources in the cluster are
	        // TODO should we do anything if the available resources are not enough?
	        Resource availableResources = amResp.getAvailableResources();
	        logger.info("Current available resources in the cluster " + availableResources);
	        
	        // Check the completed containers
	        List<ContainerStatus> completedContainers = amResp.getCompletedContainersStatuses();
	        logger.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
	        for (ContainerStatus containerStatus : completedContainers) {
	            logger.info("Got container status for containerID= " + containerStatus.getContainerId() + ", state="
	                    + containerStatus.getState() + ", exitStatus=" + containerStatus.getExitStatus()
	                    + ", diagnostics=" + containerStatus.getDiagnostics());
	            
	            // non complete containers should not be here
	            // assert (containerStatus.getState() == ContainerState.COMPLETE);
	            
	            // increment counters for completed/failed containers
	            int exitStatus = containerStatus.getExitStatus();
	            if (0 != exitStatus) {
	                // container failed
	                if (-100 != exitStatus) {
	                    // shell script failed
	                    // counts as completed
	                    numCompletedContainers.incrementAndGet();
	                    numFailedContainers.incrementAndGet();
	                } else {
	                    // something else bad happened
	                    // app job did not complete for some reason
	                    // we should re-try as the container was lost for some reason
	                    numAllocatedContainers.decrementAndGet();
	                    numRequestedContainers.decrementAndGet();
	                    // we do not need to release the container as it would be done
	                    // by the RM/CM.
	                }
	            } else {
	                // nothing to do
	                // container completed successfully
	                numCompletedContainers.incrementAndGet();
	                logger.info("Container completed successfully." + ", containerId="
	                        + containerStatus.getContainerId());
	            }	            
	        }
	        if (numCompletedContainers.get() == orcaArgs.numContainers) {
	            appDone = true;
	        }
	        
	        logger.info("Current application state: loop=" + loopCounter + ", appDone=" + appDone + ", total="
	                + orcaArgs.numContainers + ", requested=" + numRequestedContainers + ", completed="
	                + numCompletedContainers + ", failed=" + numFailedContainers + ", currentAllocated="
	                + numAllocatedContainers);	        	        
		}
		
	    // Join all launched threads
	    // needed for when we time out
	    // and we need to release containers
	    for (Thread launchThread : launchThreads) {
	        try {
	            launchThread.join(0);
	        } catch (InterruptedException e) {
	            logger.info("Exception thrown in thread join: " + e.getMessage());
	            e.printStackTrace();
	        }
	    }
	    // When the application completes, it should send a finish application signal
	    // to the RM
	    logger.info("Application completed. Signalling finish to RM");

	    FinishApplicationMasterRequest finishReq = Records.newRecord(FinishApplicationMasterRequest.class);
	    finishReq.setAppAttemptId(appAttemptID);
	    boolean isSuccess = true;
	    if (numFailedContainers.get() == 0) {
	        finishReq.setFinishApplicationStatus(FinalApplicationStatus.SUCCEEDED);
	    } else {
	        finishReq.setFinishApplicationStatus(FinalApplicationStatus.FAILED);
	        String diagnostics = "Diagnostics." + ", total=" + orcaArgs.numContainers + ", completed="
	                + numCompletedContainers.get() + ", allocated=" + numAllocatedContainers.get() + ", failed="
	                + numFailedContainers.get();
	        finishReq.setDiagnostics(diagnostics);
	        isSuccess = false;
	    }
	    resourceManager.finishApplicationMaster(finishReq);
	    return isSuccess;

	}
	
	
	/**
	 * Connect to the Resource Manager
	 * 
	 * @return Handle to communicate with the RM
	 */
	private AMRMProtocol connectToRM() {
	    YarnConfiguration yarnConf = new YarnConfiguration(conf);
	    InetSocketAddress rmAddress = yarnConf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
	            YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
	    logger.info("Connecting to ResourceManager at " + rmAddress);
	    return ((AMRMProtocol) rpc.getProxy(AMRMProtocol.class, rmAddress, conf));
	}

	/**
	 * Register the Application Master to the Resource Manager
	 * 
	 * @return the registration response from the RM
	 * @throws YarnRemoteException
	 */
	private RegisterApplicationMasterResponse registerToRM() throws YarnRemoteException {
	    RegisterApplicationMasterRequest appMasterRequest = Records.newRecord(RegisterApplicationMasterRequest.class);

	    // set the required info into the registration request:
	    // application attempt id,
	    // host on which the app master is running
	    // rpc port on which the app master accepts requests from the client
	    // tracking url for the app master
	    appMasterRequest.setApplicationAttemptId(appAttemptID);
	    appMasterRequest.setHost(appMasterHostname);
	    appMasterRequest.setRpcPort(appMasterRpcPort);
	    appMasterRequest.setTrackingUrl(appMasterTrackingUrl);

	    return resourceManager.registerApplicationMaster(appMasterRequest);
	}	

	
	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 * 
	 * @param numContainers
	 *            Containers to ask for from RM
	 * @return the setup ResourceRequest to be sent to RM
	 */
	private ResourceRequest setupContainerAskForRM(int numContainers) {
	    ResourceRequest request = Records.newRecord(ResourceRequest.class);

	    // setup requirements for hosts
	    // whether a particular rack/host is needed
	    // Refer to apis under org.apache.hadoop.net for more
	    // details on how to get figure out rack/host mapping.
	    // using * as any host will do for the distributed shell app
	    request.setHostName("*");

	    // set no. of containers needed
	    request.setNumContainers(numContainers);

	    // set the priority for the request
	    Priority pri = Records.newRecord(Priority.class);
	    // TODO - what is the range for priority? how to decide?
	    pri.setPriority(0);
	    request.setPriority(pri);

	    // Set up resource type requirements
	    // For now, only memory is supported so we set memory requirements
	    Resource capability = Records.newRecord(Resource.class);
	    capability.setMemory(containerMemory);
	    request.setCapability(capability);

	    return request;
	}	
	
	/**
	 * Ask RM to allocate given no. of containers to this Application Master
	 * 
	 * @param requestedContainers
	 *            Containers to ask for from RM
	 * @return Response from RM to AM with allocated containers
	 * @throws YarnRemoteException
	 */
	private AMResponse sendContainerAskToRM(List<ResourceRequest> requestedContainers) throws YarnRemoteException {
	    AllocateRequest req = Records.newRecord(AllocateRequest.class);
	    req.setResponseId(rmRequestID.incrementAndGet());
	    req.setApplicationAttemptId(appAttemptID);
	    req.addAllAsks(requestedContainers);
	    req.addAllReleases(releasedContainers);
	    req.setProgress((float) numCompletedContainers.get() / orcaArgs.numContainers);

	    logger.info("Sending request to RM for containers" + ", requestedSet=" + requestedContainers.size()
	            + ", releasedSet=" + releasedContainers.size() + ", progress=" + req.getProgress());

	    for (ResourceRequest rsrcReq : requestedContainers) {
	        logger.info("Requested container ask: " + rsrcReq.toString());
	    }
	    for (ContainerId id : releasedContainers) {
	        logger.info("Released container, id=" + id.getId());
	    }

	    AllocateResponse resp = resourceManager.allocate(req);
	    return resp.getAMResponse();
	}	
	
	private class LaunchContainerRunnable implements Runnable {

	    // Allocated container
	    Container container;
	    // Handle to communicate with ContainerManager
	    ContainerManager cm;

	    /**
	     * @param lcontainer
	     *            Allocated container
	     */
	    public LaunchContainerRunnable(Container lcontainer) {
	        this.container = lcontainer;
	    }
	    
	    /**
	     * Helper function to connect to CM
	     */
	    private void connectToCM() {
	        logger.debug("Connecting to ContainerManager for containerid=" + container.getId());
	        String cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
	        InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
	        logger.info("Connecting to ContainerManager at " + cmIpPortStr);
	        this.cm = ((ContainerManager) rpc.getProxy(ContainerManager.class, cmAddress, conf));
	    }
	    


		@Override
		public void run() {
	        // Connect to ContainerManager
	        connectToCM();

	        logger.info("Setting up container launch container for containerid=" + container.getId());
	        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

	        ctx.setContainerId(container.getId());
	        ctx.setResource(container.getResource());
	        
	        try {
//	            if (!Strings.isNullOrEmpty(yarnArgs.user)) {
//	                ctx.setUser(yarnArgs.user);
//	            } else {
				logger.info("Using default user name {}", UserGroupInformation.getCurrentUser().getShortUserName());
				ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
//	            }
	        } catch (IOException e) {
	            logger.info("Getting current user info failed when trying to launch the container" + e.getMessage());
	        }
	        
	        // Set the local resources
	        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();	        
	        
	        try {
	            FileSystem fs = FileSystem.get(conf);

	            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(fs.getHomeDirectory(), "/app-"
	                    + appAttemptID.getApplicationId().getId()), false);
	            while (files.hasNext()) {
	                LocatedFileStatus file = files.next();
	                LocalResource localResource = Records.newRecord(LocalResource.class);

	                localResource.setType(LocalResourceType.FILE);
	                localResource.setVisibility(LocalResourceVisibility.APPLICATION);
	                localResource.setResource(ConverterUtils.getYarnUrlFromPath(file.getPath()));
	                localResource.setTimestamp(file.getModificationTime());
	                localResource.setSize(file.getLen());
	                localResources.put(file.getPath().getName(), localResource);
	            }
	            ctx.setLocalResources(localResources);

	        } catch (IOException e1) {
	            // TODO Auto-generated catch block
	            e1.printStackTrace();
	        }

	        StringBuilder classPathEnv = new StringBuilder("${CLASSPATH}:./*");

	        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
	                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
	            classPathEnv.append(':');
	            classPathEnv.append(c.trim());
	        }

	        // classPathEnv.append(System.getProperty("java.class.path"));
	        Map<String, String> env = new HashMap<String, String>();

	        env.put("CLASSPATH", classPathEnv.toString());
	        ctx.setEnvironment(env);

	        // Set the necessary command to execute on the allocated container
	        Vector<CharSequence> vargs = new Vector<CharSequence>(5);

	        vargs.add("java");

	        vargs.add("-Xmx" + containerMemory + "m");
	 		//vargs.add("-Xdebug");
	 		//vargs.add("-Xrunjdwp:transport=dt_socket,address=8889,server=y,suspend=n");	        
	        vargs.add("org.kuttz.orca.Canary");
	        
	        logger.info("Logs found in dir : [" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "]");
	        
	        // We should redirect the output to hdfs instead of local logs
	        // so as to be able to look at the final output after the containers
	        // have been released.
	        // Could use a path suffixed with /AppId/AppAttempId/ContainerId/std[out|err]
	        vargs.add("1>/tmp/orca-node-stdout" + container.getId().getId());
	        vargs.add("2>/tmp/orca-node-stderr" + container.getId().getId());	        	        
			
	        // Get final commmand
	        StringBuilder command = new StringBuilder();
	        for (CharSequence str : vargs) {
	            command.append(str).append(" ");
	        }

	        List<String> commands = new ArrayList<String>();
	        commands.add(command.toString());
	        ctx.setCommands(commands);

	        StartContainerRequest startReq = Records.newRecord(StartContainerRequest.class);
	        startReq.setContainerLaunchContext(ctx);
	        try {
	            cm.startContainer(startReq);
	        } catch (YarnRemoteException e) {
	            logger.info("Start container failed for :" + ", containerId=" + container.getId());
	            e.printStackTrace();
	        }
	        
		}
	}

		
	
}
