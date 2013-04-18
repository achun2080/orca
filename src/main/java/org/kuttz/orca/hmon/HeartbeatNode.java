package org.kuttz.orca.hmon;

import java.util.concurrent.atomic.AtomicLong;

public class HeartbeatNode {
	
	private final int nodeId;
	private final NodeType nType;
	


	public HeartbeatNode(int nodeId, NodeType nType) {
		this.nodeId = nodeId;
		this.nType = nType;
	}
	
	public int getId() {
		return nodeId;
	}
	
	public NodeType getType() {
		return nType;
	}		

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((nType == null) ? 0 : nType.hashCode());
		result = prime * result + nodeId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HeartbeatNode other = (HeartbeatNode) obj;
		if (nType != other.nType)
			return false;
		if (nodeId != other.nodeId)
			return false;
		return true;
	}	
	
	public static class NodeState {
		public volatile AtomicLong timeStamp = new AtomicLong(0);
		public volatile NodeInfo nodeInfo;
		public volatile String host;
		public volatile int port;
	}

}
