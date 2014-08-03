package akka.contrib.cluster.topology

import com.typesafe.config.ConfigFactory
import akka.testkit.AkkaSpec

/**
 *
 *
 * @author Lukasz Olczak
 */
object ClusterTopologySpec {

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

}

class ClusterTopologySpec extends AkkaSpec {

  import ClusterTopologySpec._

  "A ClusterTopologySettings" must {

    "parse topology configuration" in {
      val config = ConfigFactory.parseString(topologyConfig)
      val clusterTopology = ClusterTopology.fromConfig(config.getConfig("akka"))
      clusterTopology shouldNot be(null)
      clusterTopology.containsZone("1") should be(true)
      clusterTopology.containsZone("2") should be(true)
      clusterTopology.containsZone("3") should be(true)
    }
  }
}
