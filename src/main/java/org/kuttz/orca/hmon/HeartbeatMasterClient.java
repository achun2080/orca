package org.kuttz.orca.hmon;

public interface HeartbeatMasterClient {
	
	public void nodeWarn(NodeType nType, HeartbeatNode node);
	
	public void nodeDead(NodeType nType, HeartbeatNode node);

}
