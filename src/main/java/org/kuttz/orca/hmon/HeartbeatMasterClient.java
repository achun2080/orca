package org.kuttz.orca.hmon;

import org.kuttz.orca.hmon.HeartbeatNode.NodeState;

public interface HeartbeatMasterClient {
	
	public void nodeUp(HeartbeatNode node, NodeState firstNodeState);
	
	public void nodeWarn(HeartbeatNode node, NodeState lastNodeState);
	
	public void nodeDead(HeartbeatNode node, NodeState lastNodeState);

}
