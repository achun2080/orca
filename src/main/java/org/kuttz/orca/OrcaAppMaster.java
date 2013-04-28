package org.kuttz.orca;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
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
import org.kuttz.orca.controller.OrcaController;
import org.kuttz.orca.controller.OrcaControllerArgs;
import org.kuttz.orca.controller.OrcaControllerClient;
import org.kuttz.orca.controller.OrcaLaunchContext;
import org.kuttz.orca.controller.OrcaController.ControllerRequest;
import org.kuttz.orca.controller.OrcaController.Node;
import org.kuttz.orca.controller.OrcaController.ControllerRequest.ReqType;
import org.kuttz.orca.hmon.HeartbeatMasterClient;
import org.kuttz.orca.hmon.HeartbeatNode;
import org.kuttz.orca.hmon.HeartbeatNode.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrcaAppMaster implements OrcaControllerClient, HeartbeatMasterClient {

	private static Logger logger = LoggerFactory.getLogger(OrcaAppMaster.class);
	
	private final OrcaControllerArgs orcaArgs;
	
	private OrcaController oc;
	
	// Incremental counter for rpc calls to the RM
	private final AtomicInteger rmRequestID = new AtomicInteger();	
	
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
	
	// Containers to be released
	private final CopyOnWriteArrayList<ContainerId> releasedContainers = new CopyOnWriteArrayList<ContainerId>();	
	
	private int containerMemory;
	
	private ExecutorService tp = Executors.newCachedThreadPool();
	
	private RequestContainerRunnable requester = new RequestContainerRunnable();
	
	@Override
	public HeartbeatMasterClient getHeartbeatClient() {
		return this;
	}

	@Override
	public ExecutorService getExecutorService() {
		return this.tp;
	}
	
	public static void main(String[] args) {
		logger.info("Starting Orca Application Master..");
		
		OrcaControllerArgs orcaArgs = new OrcaControllerArgs();
		logger.info("OrcaAM args = " + Arrays.toString(args));
		Tools.parseArgs(orcaArgs, args);
		
		OrcaAppMaster appMaster = new OrcaAppMaster(orcaArgs);
		try {
			appMaster.init();
			appMaster.run();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error running Orca ApplicationMaster", e);
			System.exit(1);
		}
		logger.info("Orca ApplicationMaster completed successfully..");
		System.exit(0);				
	}	
	
	public OrcaAppMaster(OrcaControllerArgs orcaArgs) {
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
		
		this.tp.submit(requester);
		this.oc = new OrcaController(this.orcaArgs, this);
	}
	
	public void run() throws YarnRemoteException {
		logger.info("Starting ApplicationMaster");
		
		oc.init();
		this.tp.submit(oc);		
		
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
		
		while(true) {
			ControllerRequest req = null;
			try {
				req = oc.getNextRequest();
			} catch (InterruptedException e1) {
				logger.info("Got InterruptException !!" );
			}
			logger.info("Got Request from OrcaController[" + req.getType() + "]" );
			if ((req != null) && req.getType().equals(ReqType.CONTAINER)) {
				requester.inQ.add(req);
			} else {
				handleLaunchRequest(req);
			}

		}
		
	}

	private void handleLaunchRequest(ControllerRequest req) {
		OrcaLaunchContext launchContext = req.getLaunchContext();
		ContainerNode requestNode = (ContainerNode)req.getRequestNode();
		tp.submit(new LaunchContainerRunnable(launchContext, requestNode.container));
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
//	    req.setProgress((float) numCompletedContainers.get() / orcaArgs.numContainers);

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

	@Override
	public void nodeUp(HeartbeatNode node, NodeState firstNodeState) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nodeWarn(HeartbeatNode node, NodeState lastNodeState) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nodeDead(HeartbeatNode node, NodeState lastNodeState) {
		// TODO Auto-generated method stub
		
	}

	
	public static class ContainerNode implements Node {

		public final Container container;
		
		public ContainerNode(Container container) {
			this.container = container;
		}
		
		@Override
		public int getId() {
			return container.getId().getId();
		}
		
	}
	
	private class RequestContainerRunnable implements Runnable {
		private final LinkedBlockingQueue<ControllerRequest> inQ = new LinkedBlockingQueue<ControllerRequest>();
		private int exCount = 0;
		@Override
		
		public void run() {
			while (true) {
				if (inQ.size() != 0) {
					LinkedList<ControllerRequest> outstanding = new LinkedList<ControllerRequest>();
					inQ.drainTo(outstanding);
					List<ResourceRequest> resourceReq = new ArrayList<ResourceRequest>();
					ResourceRequest containerAsk = OrcaAppMaster.this.setupContainerAskForRM(outstanding.size());
					resourceReq.add(containerAsk);
					
					// Send request to RM
					logger.info("Asking RM for a container !!");
					AMResponse amResp = null;
					try {
						amResp = sendContainerAskToRM(resourceReq);
					} catch (YarnRemoteException e) {						
						logger.error("Got Exception while requesting Container !!", e);
						if (exCount++ > 10) {
							System.exit(-1);
						}
					}
					// Retrieve list of allocated containers from the response
					List<Container> allocatedContainers = amResp.getAllocatedContainers();					
					logger.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
					for (Container allocatedContainer : allocatedContainers) {
						ControllerRequest req = outstanding.poll();
						if (req != null) {
							req.setResponse(new ContainerNode(allocatedContainer));							
						}
					}
					while (outstanding.size() > 0) {
						inQ.add(outstanding.poll());
					}
				}
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// Don't care..
				}
			}
			
		}
	}
	
	private class LaunchContainerRunnable implements Runnable {

		final OrcaLaunchContext launchContext;
	    // Allocated container
	    final Container container;
	    // Handle to communicate with ContainerManager
	    ContainerManager cm;

	    /**
	     * @param lcontainer
	     *            Allocated container
	     */
	    public LaunchContainerRunnable(OrcaLaunchContext launchContext, Container lcontainer) {
	        this.container = lcontainer;
	        this.launchContext = launchContext;
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
				logger.info("Using default user name {}", UserGroupInformation.getCurrentUser().getShortUserName());
				ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
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
	        Map<String, String> env = launchContext.getEnv();

	        env.put("CLASSPATH", classPathEnv.toString());
	        ctx.setEnvironment(env);

	        List<String> commands = new ArrayList<String>();
	        commands.add(launchContext.getShellCommand());
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
