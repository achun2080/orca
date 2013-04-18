/** Thrift specification for the ELB proxy */
namespace  java   org.kuttz.orca.proxy

service TProxyCommandEndPoint {
    /** Add node, returns success/failure */
    bool addNode(1: string host, 2: i32 port, 3: i32 weight),

    /** Remove node, return success/failure */
    bool removeNode(1: string host, 2: i32 port)

}
