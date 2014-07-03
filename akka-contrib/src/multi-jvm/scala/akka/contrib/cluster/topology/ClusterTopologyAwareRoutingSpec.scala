package akka.contrib.cluster.topology

import akka.remote.testkit.{ STMultiNodeSpec, MultiNodeSpec, MultiNodeConfig }
import com.typesafe.config.ConfigFactory
import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.testkit.ImplicitSender

/**
 *
 *
 * @author Lukasz Olczak
 */
object ClusterTopologyAwareRoutingSpec extends MultiNodeConfig {

  val zone1 = new {
    val node1 = role("zone1-node1")
    val node2 = role("zone1-node2")
  }

  val zone2 = new {
    val node1 = role("zone2-node1")
    val node2 = role("zone2-node2")
  }

  val zone3 = new {
    val node1 = role("zone3-node1")
    val node2 = role("zone3-node2")
  }

  commonConfig(ConfigFactory.parseString(
    """
      |akka {
      |   loglevel = INFO
      |   actor {
      |     provider = "akka.cluster.ClusterActorRefProvider"
      |   }
      |   remote {
      |     log-remote-lifecycle-events = off
      |     netty.tcp {
      |       hostname = "127.0.0.1"
      |       port = 2222
      |     }
      |   }
      |   cluster {
      |     jmx.enabled                         = off
      |     gossip-interval                     = 200 ms
      |     leader-actions-interval             = 200 ms
      |     unreachable-nodes-reaper-interval   = 500 ms
      |     periodic-tasks-initial-delay        = 300 ms
      |     publish-stats-interval              = 0 s # always, when it happens
      |     failure-detector.heartbeat-interval = 500 ms
      |
      |     topology {
      |            zones = [{
      |                id = "1"
      |                zone-classifier = "net:127.1.0.1/16"
      |                proximity-list = [3,2]
      |            },
      |            {
      |                id = "2"
      |                zone-classifier = "net:127.2.0.1/16"
      |                proximity-list = [1,3]
      |            },
      |            {
      |                id = "3"
      |                zone-classifier = "net:127.3.0.1/16"
      |                proximity-list = [1,2]
      |            }
      |            ]
      |        }
      |   }
      |}
    """.stripMargin))

  nodeConfig(zone1.node1) {
    ConfigFactory.parseString("""akka.remote.netty.tcp.hostname = "127.1.0.1" """)
  }
  nodeConfig(zone1.node2) {
    ConfigFactory.parseString("""akka.remote.netty.tcp.hostname = "127.1.0.2" """)
  }
  nodeConfig(zone2.node1) {
    ConfigFactory.parseString("""akka.remote.netty.tcp.hostname = "127.2.0.1" """)
  }
  nodeConfig(zone2.node2) {
    ConfigFactory.parseString("""akka.remote.netty.tcp.hostname = "127.2.0.2" """)
  }
  nodeConfig(zone3.node1) {
    ConfigFactory.parseString("""akka.remote.netty.tcp.hostname = "127.3.0.1" """)
  }
  nodeConfig(zone3.node2) {
    ConfigFactory.parseString("""akka.remote.netty.tcp.hostname = "127.3.0.2" """)
  }
}

class ClusterTopologyAwareRoutingMultiJvmZone1Node1 extends ClusterTopologyAwareRoutingSpec

class ClusterTopologyAwareRoutingMultiJvmZone1Node2 extends ClusterTopologyAwareRoutingSpec

class ClusterTopologyAwareRoutingMultiJvmZone2Node1 extends ClusterTopologyAwareRoutingSpec

class ClusterTopologyAwareRoutingMultiJvmZone2Node2 extends ClusterTopologyAwareRoutingSpec

class ClusterTopologyAwareRoutingMultiJvmZone3Node1 extends ClusterTopologyAwareRoutingSpec

class ClusterTopologyAwareRoutingMultiJvmZone3Node2 extends ClusterTopologyAwareRoutingSpec

class ClusterTopologyAwareRoutingSpec extends MultiNodeSpec(ClusterTopologyAwareRoutingSpec) with STMultiNodeSpec with ImplicitSender {

  import scala.concurrent.duration._
  import ClusterTopologyAwareRoutingSpec._

  override def initialParticipants = roles.size

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
      //      createCoordinator()
    }
    enterBarrier(from.name + "-joined")
  }

  "A ClusterClient" must {

    "startup cluster" in within(30 seconds) {
      //      awaitClusterUp(roles: _*)
      println("test§1")
      join(zone1.node2, zone1.node1)
      join(zone2.node1, zone1.node1)
      join(zone2.node2, zone1.node1)
      join(zone3.node1, zone1.node1)
      join(zone3.node2, zone1.node1)
      enterBarrier("after-1")
    }
  }

}
