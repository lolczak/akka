package akka.contrib.cluster.routing

import akka.testkit.AkkaSpec
import akka.contrib.cluster.topology.ClusterTopology
import com.typesafe.config.ConfigFactory
import akka.routing.ActorSelectionRoutee
import org.scalatest.mock.MockitoSugar

/**
 *
 *
 * @author Lukasz Olczak
 */

object RouteeTopologySpec {

  val topologyConfig =
    """
      |akka {
      |    cluster {
      |        topology {
      |            zones = [{
      |                id = "1"
      |                zone-classifier = "net:213.168.0.0/8"
      |                proximity-list = [3,2]
      |            },
      |            {
      |                id = "2"
      |                zone-classifier = "net:213.169.0.0/8"
      |                proximity-list = [1,3]
      |            },
      |            {
      |                id = "3"
      |                zone-classifier = "net:213.170.0.0/8"
      |                proximity-list = [1,2]
      |            }
      |            ]
      |        }
      |    }
      |}
    """.stripMargin

  val SampleClusterTopology = ClusterTopology.fromConfig(ConfigFactory.parseString(topologyConfig))

}

class RouteeTopologySpec extends AkkaSpec with MockitoSugar {
  //
  //  val sampleRoutees = IndexedSeq(
  //    ActorSelectionRoutee(system.actorSelection("akka://system@host:port"))
  //  )
  //
  //  "A routee topology " must {
  //
  //    "return self zone of callee" in {
  //       val topology = new RouteeTopology(,)
  //    }
  //
  //    "return closest routees" in {
  //
  //    }
  //
  //  }

}
