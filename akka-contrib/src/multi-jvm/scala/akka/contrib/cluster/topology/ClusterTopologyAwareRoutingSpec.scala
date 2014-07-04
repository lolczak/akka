package akka.contrib.cluster.topology

import akka.remote.testkit.{ STMultiNodeSpec, MultiNodeSpec, MultiNodeConfig }
import com.typesafe.config.ConfigFactory
import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.testkit.{ TestProbe, DefaultTimeout, LongRunningTest, ImplicitSender }
import akka.actor.{ ActorRef, Props, Actor, Address }
import akka.routing.{ GetRoutees, Routees, RoutedActorRef, FromConfig }
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp }
import scala.concurrent.Await
import akka.pattern.ask

/**
 *
 *
 * @author Lukasz Olczak
 */
object ClusterTopologyAwareRoutingSpec extends MultiNodeConfig {

  class Echo extends Actor {
    def receive = {
      case msg ⇒ {
        println(s"Address=${Cluster(context.system).selfAddress} received $msg")
        sender() ! Reply(Cluster(context.system).selfAddress)
      }
    }
  }

  final case class Reply(address: Address)

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

  commonConfig(
    ConfigFactory.parseString(
      """
        |akka {
        |   loglevel = INFO
        |   actor {
        |     provider = "akka.cluster.ClusterActorRefProvider"
        |
        |     router.type-mapping {
        |       topology-aware-pool = "akka.contrib.cluster.routing.TopologyAwareRoutingPool"
        |       topology-aware-group = "akka.contrib.cluster.routing.TopologyAwareRoutingGroup"
        |     }
        |
        |     deployment {
        |       /router1 {
        |         router = topology-aware-pool
        |         zone-routing-logic = closest-zone
        |         node-routing-logic = random
        |         nr-of-instances = 6
        |         cluster {
        |           enabled = on
        |           max-nr-of-instances-per-node = 1
        |           allow-local-routees = on
        |         }
        |       }
        |     }
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
        |       zones = [{
        |         id = "1"
        |         zone-classifier = "net:127.1.0.1/16"
        |         proximity-list = [3,2]
        |       },
        |       {
        |         id = "2"
        |         zone-classifier = "net:127.2.0.1/16"
        |         proximity-list = [1,3]
        |       },
        |       {
        |         id = "3"
        |         zone-classifier = "net:127.3.0.1/16"
        |         proximity-list = [1,2]
        |       }
        |       ]
        |     }
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

class ClusterTopologyAwareRoutingSpec extends MultiNodeSpec(ClusterTopologyAwareRoutingSpec)
  with STMultiNodeSpec with ImplicitSender with DefaultTimeout {

  import scala.concurrent.duration._
  import ClusterTopologyAwareRoutingSpec._

  override def initialParticipants = roles.size

  lazy val router1 = system.actorOf(FromConfig.props(Props[Echo]), "router1")

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      Cluster(system) join node(to).address
      //      createCoordinator()
    }
    enterBarrier(from.name + "-joined")
  }

  def awaitMemberUp(memberProbe: TestProbe, nodes: RoleName*): Unit = {
    runOn(nodes.filterNot(_ == nodes.head): _*) {
      memberProbe.expectMsgType[MemberUp](15.seconds).member.address should be(node(nodes.head).address)
    }
    runOn(nodes.head) {
      memberProbe.receiveN(nodes.size, 15.seconds).collect { case MemberUp(m) ⇒ m.address }.toSet should be(
        nodes.map(node(_).address).toSet)
    }
    enterBarrier(nodes.head.name + "-up")
  }

  implicit def address(role: RoleName): Address = node(role).address

  def receiveReplies(expectedReplies: Int): Map[Address, Int] = {
    val zero = Map.empty[Address, Int] ++ roles.map(address(_) -> 0)
    (receiveWhile(5 seconds, messages = expectedReplies) {
      case Reply(address) ⇒ address
    }).foldLeft(zero) {
      case (replyMap, address) ⇒ replyMap + (address -> (replyMap(address) + 1))
    }
  }

  def currentRoutees(router: ActorRef) =
    Await.result(router ? GetRoutees, timeout.duration).asInstanceOf[Routees].routees

  "A cluster topology router" must {

    "startup cluster with all nodes" in within(30 seconds) {
      val memberProbe = TestProbe()
      Cluster(system).subscribe(memberProbe.ref, classOf[MemberUp])
      memberProbe.expectMsgClass(classOf[CurrentClusterState])

      join(zone1.node1, zone1.node1)
      awaitMemberUp(memberProbe, zone1.node1)
      join(zone1.node2, zone1.node1)
      awaitMemberUp(memberProbe, zone1.node2, zone1.node1)
      join(zone2.node1, zone1.node1)
      awaitMemberUp(memberProbe, zone2.node1, zone1.node2, zone1.node1)
      join(zone2.node2, zone1.node1)
      awaitMemberUp(memberProbe, zone2.node2, zone2.node1, zone1.node2, zone1.node1)
      join(zone3.node1, zone1.node1)
      awaitMemberUp(memberProbe, zone3.node1, zone2.node2, zone2.node1, zone1.node2, zone1.node1)
      join(zone3.node2, zone1.node1)
      awaitMemberUp(memberProbe, zone3.node2, zone3.node1, zone2.node2, zone2.node1, zone1.node2, zone1.node1)
      enterBarrier("after-1")
    }

    "deploy routees to the member nodes in the cluster" taggedAs LongRunningTest in {
      runOn(zone1.node1) {
        router1.isInstanceOf[RoutedActorRef] should be(true)

        awaitAssert(currentRoutees(router1).size should be(6))

        (1 to 10).foreach(i ⇒ router1 ! s"hit $i")

        val replies = receiveReplies(10)
        replies(zone1.node1) should be(0)
        replies(zone1.node2) should be(0)
        replies(zone2.node1) should be(0)
        replies(zone2.node2) should be(0)
        replies(zone3.node1) + replies(zone3.node2) should be(10)
      }
      enterBarrier("after-2")
    }

  }
}
