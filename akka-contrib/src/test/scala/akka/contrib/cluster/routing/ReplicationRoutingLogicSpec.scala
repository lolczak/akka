package akka.contrib.cluster.routing

import akka.testkit.AkkaSpec
import org.scalatest.mock.MockitoSugar
import akka.routing.{ ActorSelectionRoutee, Routee }
import akka.actor.{ Address, ActorPath, ActorSelection }
import org.mockito.Mockito._
import akka.contrib.cluster.topology.ClusterTopology
import com.typesafe.config.ConfigFactory
import scala.collection.immutable.IndexedSeq
import java.lang.IllegalArgumentException

/**
 *
 *
 * @author Lukasz Olczak
 */

object ReplicationRoutingLogicSpec extends MockitoSugar {

  val topologyConfig =
    """
      |akka {
      |    cluster {
      |        topology {
      |            zones = [{
      |                id = "zone1"
      |                zone-classifier = "net:127.1.0.1/16"
      |                proximity-list = [3,2]
      |            },
      |            {
      |                id = "zone2"
      |                zone-classifier = "net:127.2.0.1/16"
      |                proximity-list = [1,3]
      |            },
      |            {
      |                id = "zone3"
      |                zone-classifier = "net:127.3.0.1/16"
      |                proximity-list = [2,1]
      |            }
      |            ]
      |        }
      |    }
      |}
    """.stripMargin

  val SampleClusterTopology = ClusterTopology.fromConfig(ConfigFactory.parseString(topologyConfig).getConfig("akka.cluster.topology"))

  val SelfAddress = Address("akka", "testSystem", "127.1.0.1", 12)

  val Zone1Node1Routee = testRoutee("127.1.0.1")

  val Zone1Node2Routee = testRoutee("127.1.0.2")

  val Zone2Node1Routee = testRoutee("127.2.0.1")

  val Zone2Node2Routee = testRoutee("127.2.0.2")

  val Zone3Node1Routee = testRoutee("127.3.0.1")

  val Zone3Node2Routee = testRoutee("127.3.0.2")

  val TestRoutees = IndexedSeq(Zone1Node1Routee, Zone1Node2Routee, Zone2Node1Routee, Zone2Node2Routee, Zone3Node1Routee, Zone3Node2Routee)

  def testRoutee(host: String): Routee = {
    val address = Address("akka", "testSystem", host, 12)
    val actorSelection = mock[ActorSelection]
    val actorPath = mock[ActorPath]
    when(actorPath.address) thenReturn address
    when(actorSelection.anchorPath) thenReturn actorPath
    ActorSelectionRoutee(actorSelection)
  }

}

class ReplicationRoutingLogicSpec extends AkkaSpec with MockitoSugar {

  import ReplicationRoutingLogicSpec._

  "A replication routing logic" must {

    "select number of routees equal to global replication routing logic" in {
      val replicationLogic = ReplicationRoutingLogic(3, None)
      val topology = new TopologyAwareRouteesImpl(TestRoutees, SampleClusterTopology, SelfAddress)
      val selectedRoutees = replicationLogic.select(mock[AnyRef], topology)
      selectedRoutees.size should be(3)
    }

    "throw IllegalArgumentException if a number of routees is lesser than global replication factor" in {
      intercept[IllegalArgumentException] {
        val replicationLogic = ReplicationRoutingLogic(3, None)
        val topology = new TopologyAwareRouteesImpl(IndexedSeq.empty, SampleClusterTopology, SelfAddress)
        replicationLogic.select(mock[AnyRef], topology)
      }
    }

    "select routees from each zone such that it satisfies the zone replication factor criteria " in {
      val replicationLogic = ReplicationRoutingLogic(3, Some(Map("zone1" -> 2, "zone2" -> 1, "zone3" -> 0)))

      val topology = new TopologyAwareRouteesImpl(TestRoutees, SampleClusterTopology, SelfAddress)
      val selectedRoutees = replicationLogic.select(mock[AnyRef], topology)

      selectedRoutees should (
        contain(Zone1Node1Routee) and
        contain(Zone1Node2Routee) and
        contain oneOf (Zone2Node1Routee, Zone2Node2Routee) and
        have size (3))
    }

    "throw IllegalArgumentException if a global replication factor is not equal to a sum of zone replication factors" in {
      intercept[IllegalArgumentException] {
        ReplicationRoutingLogic(5, Some(Map("zone1" -> 2, "zone2" -> 1, "zone3" -> 0)))
      }

      intercept[IllegalArgumentException] {
        ReplicationRoutingLogic(1, Some(Map("zone1" -> 2, "zone2" -> 1, "zone3" -> 0)))
      }
    }

    "throw IllegalArgumentException if a number of routees is lesser than zone replication factor" in {
      intercept[IllegalArgumentException] {
        val replicationLogic = ReplicationRoutingLogic(3, Some(Map("zone1" -> 3, "zone2" -> 0, "zone3" -> 0)))

        val topology = new TopologyAwareRouteesImpl(TestRoutees, SampleClusterTopology, SelfAddress)
        replicationLogic.select(mock[AnyRef], topology)
      }
    }

  }

}
