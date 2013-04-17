/** Thrift specification for Orca heartbeat protocol */
namespace  java   org.kuttz.orca.hmon

enum NodeType {
    CONTAINER = 0;
    PROXY = 1;
}

struct ContainerMetrics {
    /** Latency Average */
    1: optional i32 latencyAvg,

    /** Latency 95th */
    2: optional i32 latency95,
}

struct ProxyMetrics {
    /** Latency Average */
    1: optional i32 latencyAvg,

    /** Latency 95th */
    2: optional i32 latency95,
}


struct NodeMetrics {
    /** State of Container */
    1: optional ContainerMetrics containerMetrics,

    /** State of Proxy */
    2: optional ProxyMetrics proxyMetrics,
}


struct HeartbeatMsg {
    /** Slave Node id */
    1: required i32 nodeId,

    /** Slave Node name */
    2: required NodeType nodeType,

    /** Slave Node state */
    3: required NodeMetrics nodeMetrics,
}

service THeartbeatService {
    /** Accept Heartbeat msg */
    bool acceptHeartbeat(1: HeartbeatMsg hbMsg)

}
