# Nimbus
Nimbus is a distributed key-value store written in Go, designed for scalability, fault tolerance, and high availability. It combines the gossip protocol for peer-to-peer communication, consistent hashing for efficient data distribution, and lightweight Paxos for strong consistency. Currently a work in progress, Nimbus aims to deliver a fast and reliable solution for managing distributed data in dynamic environments.

## Key Features
### The Gossip Protocol
Nimbus employs a **gossip protocol** to efficiently disseminate knowledge about the cluster, ensuring a resilient and flexible communication mechanism. The implementation is decoupled from the application logic, enabling any node to participate in gossip irrespective of its role.

#### Node Roles
- **Worker Nodes**: Handle key-value storage and perform core data operations.
- **Proxy Nodes**: Act as reverse proxies, routing client requests to the appropriate worker nodes.

#### Knowledge Dissemination Strategies
1. **Random Strategy**:
   - Each node selects a peer randomly.
   - The selected peer receives a "spread" request to update its knowledge.

2. **Round-Robin Strategy**:
   - Each node maintains a queue of other nodes.
   - Nodes make "spread" requests in sequential order through the queue.

#### Handling Unreachable Nodes
- Nodes that fail to respond with an acknowledgment to a "spread" request are marked as **unreachable**.
- **Unreachable nodes**:
  - Are excluded from handling storage requests.
  - Remain labeled as unreachable until they recover and rejoin the cluster.

#### Spread Requests
- Spread requests propagate versioned knowledge of the cluster:
  - If the receiving node’s knowledge is outdated, it initiates an additional request to update itself.
  - This ensures synchronization between nodes.

#### Catchup Requests
- Used by nodes when they first join the cluster to gain an initial understanding of the cluster state.
- Two modes for initiating catchup:
  1. **Initiator Address**:
     - The new node specifies the address of an existing node in the cluster to bootstrap knowledge.
  2. **Multicast Protocol**:
     - The new node uses multicast to discover other nodes in the cluster and synchronize with them.

By separating the gossip protocol from specific node roles, Nimbus achieves high flexibility and ensures that the cluster remains cohesive even in dynamic environments.

### Lightweight Paxos

Nimbus uses a **lightweight implementation of Paxos** to ensure consistency across nodes while optimizing for performance. The protocol is triggered during key-value operations, particularly when a request to set a key is made. The steps involved depend on the consistency level and replication factor, which determine the number of nodes required to achieve consensus.

#### Key Steps in Lightweight Paxos

1. **Prepare Request**:
   - **Purpose**: Ensures that no conflicting proposals exist before proceeding.
   - The leader node sends a "prepare" request to the required number of nodes.
   - Nodes respond with either:
     - Their current highest proposal number.
     - A promise not to accept proposals with lower numbers.
   - **Why Necessary**: Prevents conflicting updates and ensures that if an earlier proposal was partially accepted, it can be honored.

2. **Accept Request**:
   - **Purpose**: Proposes the new value to the participating nodes.
   - After receiving sufficient promises in the prepare phase, the leader sends an "accept" request with the new value and proposal number.
   - Nodes respond by:
     - Accepting the proposal if it matches their current promise.
   - **Why Necessary**: Establishes a majority agreement on the proposed value, ensuring the system reaches a consistent state.

3. **Commit Request**:
   - **Purpose**: Finalizes the operation and applies the change.
   - Once the proposal is accepted by the majority of required nodes, the leader sends a "commit" request to inform all participating nodes to persist the value.
   - **Why Necessary**: Ensures the value is stored and replicated consistently across nodes.

#### Calculating the Minimum Required Nodes
- **Consistency Level**:
  - Determines the strictness of the operation (e.g., strong, eventual).
- **Replication Factor**:
  - Defines the number of nodes the key is replicated to.
- Based on these parameters, Nimbus calculates the minimum number of nodes required to proceed with the Paxos process.

By incorporating lightweight Paxos, Nimbus achieves a balance between ensuring data consistency and maintaining low-latency operations, tailoring the protocol's rigor to the application's needs.

### Consistent Hashing

Nimbus employs **consistent hashing** to distribute keys across nodes, ensuring balanced data allocation and minimizing disruptions during cluster scaling. However, this component is one of the most **work-in-progress** features in Nimbus, with certain capabilities yet to be implemented.

#### Current Features
- **Multiple Tokens per Node**:
  - Each node can hold a large number of tokens, ensuring a more uniform distribution of keys across the cluster.
  - This reduces the risk of data hotspots and improves load balancing.

#### Limitations (To Be Developed)
- **Token Redistribution**:
  - Currently, Nimbus does not support redistributing tokens when nodes are added or removed from the cluster.
  - This feature is planned for future development to enhance scalability and fault tolerance.

By leveraging consistent hashing with multiple tokens per node, Nimbus ensures fair key distribution in its current state, with room for future enhancements to support dynamic token management.
