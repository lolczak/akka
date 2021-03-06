package akka.contrib.cluster.routing

import akka.remote.testkit.{ STMultiNodeSpec, MultiNodeConfig }
import com.typesafe.config.ConfigFactory
import akka.cluster.Cluster
import akka.testkit.{ DefaultTimeout, LongRunningTest, ImplicitSender }
import akka.routing.{ GetRoutees, RoutedActorRef }
import scala.concurrent.Await
import akka.pattern.ask
import akka.actor.Actor
import akka.actor.Address
import akka.remote.testkit.MultiNodeSpec
import akka.routing.FromConfig
import akka.actor.Props
import akka.remote.testconductor.RoleName
import akka.testkit.TestProbe
import akka.cluster.ClusterEvent.MemberUp
import akka.actor.ActorRef
import akka.routing.Routees
import akka.cluster.ClusterEvent.CurrentClusterState

/**
 *
 *
 * @author Lukasz Olczak
 */
object TopologyAwareRoutingSpec extends MultiNodeConfig {

  val MessageCount = 10

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
        |         routing-logic = random-closest
        |         nr-of-instances = 6
        |         cluster {
        |           enabled = on
        |           max-nr-of-instances-per-node = 1
        |           allow-local-routees = off
        |         }
        |       }
        |       /router2 {
        |         router = topology-aware-group
        |         routing-logic = random-closest
        |         nr-of-instances = 10
        |         routees.paths = [ "/user/service" ]
        |         cluster {
        |           enabled = on
        |           max-nr-of-instances-per-node = 1
        |           allow-local-routees = off
        |           use-role = service
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
        |         proximity-list = [2,1]
        |       }
        |       ]
        |     }
        |   }
        |}
      """.stripMargin))

  nodeConfig(zone1.node1) {
    ConfigFactory.parseString(
      """
        |akka.remote.netty.tcp.hostname = "127.1.0.1"
        |akka.cluster.roles =["service"]
      """.stripMargin)
  }
  nodeConfig(zone1.node2) {
    ConfigFactory.parseString(
      """
        |akka.remote.netty.tcp.hostname = "127.1.0.2"
        |akka.cluster.roles =["service"]
      """.stripMargin)
  }
  nodeConfig(zone2.node1) {
    ConfigFactory.parseString(
      """
        |akka.remote.netty.tcp.hostname = "127.2.0.1"
        |akka.cluster.roles =["service"]
      """.stripMargin)
  }
  nodeConfig(zone2.node2) {
    ConfigFactory.parseString(
      """
        |akka.remote.netty.tcp.hostname = "127.2.0.2"
        |akka.cluster.roles =["service"]
      """.stripMargin)
  }
  nodeConfig(zone3.node1) {
    ConfigFactory.parseString("""akka.remote.netty.tcp.hostname = "127.3.0.1" """)
  }
  nodeConfig(zone3.node2) {
    ConfigFactory.parseString("""akka.remote.netty.tcp.hostname = "127.3.0.2" """)
  }

}

class TopologyAwareRoutingMultiJvmZone1Node1 extends TopologyAwareRoutingSpec
class TopologyAwareRoutingMultiJvmZone1Node2 extends TopologyAwareRoutingSpec
class TopologyAwareRoutingMultiJvmZone2Node1 extends TopologyAwareRoutingSpec
class TopologyAwareRoutingMultiJvmZone2Node2 extends TopologyAwareRoutingSpec
class TopologyAwareRoutingMultiJvmZone3Node1 extends TopologyAwareRoutingSpec
class TopologyAwareRoutingMultiJvmZone3Node2 extends TopologyAwareRoutingSpec

class TopologyAwareRoutingSpec extends MultiNodeSpec(TopologyAwareRoutingSpec)
  with STMultiNodeSpec with ImplicitSender with DefaultTimeout {

  import scala.concurrent.duration._
  import TopologyAwareRoutingSpec._

  override def initialParticipants = roles.size

  lazy val router1 = system.actorOf(FromConfig.props(Props[Echo]), "router1")

  lazy val router2 = system.actorOf(FromConfig.props(), "router2")

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
      memberProbe.receiveN(nodes.size, 15.seconds).collect {
        case MemberUp(m) ⇒ m.address
      }.toSet should be(
        nodes.map(node(_).address).toSet)
    }
    enterBarrier(nodes.head.name + "-up")
  }

  implicit def address(role: RoleName): Address = node(role).address

  def receiveReplies(expectedReplies: Int): Map[Address, Int] = {
    val zero = Map.empty[Address, Int] ++ roles.map(address(_) -> 0)
    (receiveWhile(15 seconds, messages = expectedReplies) {
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

        awaitAssert(currentRoutees(router1).size should be(5))

        (1 to MessageCount).foreach(i ⇒ router1 ! s"hit $i")

        val replies = receiveReplies(MessageCount)
        replies(zone1.node1) should be(0)
        replies(zone1.node2) should be(MessageCount)
        replies(zone2.node1) should be(0)
        replies(zone2.node2) should be(0)
        replies(zone3.node1) should be(0)
        replies(zone3.node2) should be(0)
      }
      enterBarrier("after-2")
    }

    "lookup routees on the member nodes in the cluster" taggedAs LongRunningTest in {
      runOn(zone2.node2, zone2.node1, zone1.node2, zone1.node1) {
        system.actorOf(Props(classOf[Echo]), "service")
      }
      enterBarrier("service-started")

      runOn(zone3.node2) {
        awaitAssert(currentRoutees(router2).size should be(4))

        (1 to MessageCount).foreach(i ⇒ router2 ! s"hit $i")

        val replies = receiveReplies(MessageCount)
        replies(zone1.node1) should be(0)
        replies(zone1.node2) should be(0)
        replies(zone2.node1) + replies(zone2.node2) should be(MessageCount)
        replies(zone3.node1) should be(0)
        replies(zone3.node2) should be(0)
      }
      enterBarrier("after-2")
    }

  }
}

