/** Thrift specification for Orca heartbeat protocol */
namespace  java   org.kuttz.orca.hmon

enum NodeType {
    CONTAINER = 0;
    PROXY = 1;
}

struct NodeInfo {
    /** State of Node - any json object */
    1: optional string infoJson,

    /** Port of some other service that exposes an End point on the node */
    2: optional i32 auxEndPointPort1,
    /** Port of some other service that exposes an End point on the node */
    3: optional i32 auxEndPointPort2,
}


struct HeartbeatMsg {
    /** Slave Node id */
    1: required i32 nodeId,

    /** Slave hostname */
    2: required string host,

    /** Slave Node name */
    3: required NodeType nodeType,

    /** Slave Node Info */
    4: required NodeInfo nodeInfo,

    /** Port on which Command EndPoint will listen on*/
    5: optional i32 commandPort,
}

service THeartbeatEndPoint {
    /** Accept Heartbeat msg */
    bool acceptHeartbeat(1: HeartbeatMsg hbMsg)
}

service THeartbeatCommandEndPoint {
    /** Notify Slave of change in Heartbeat End point */
    bool changeEndPoint(1: string host, 2: i32 port)
}
