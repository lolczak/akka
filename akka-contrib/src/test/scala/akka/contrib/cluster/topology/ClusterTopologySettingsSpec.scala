package akka.contrib.cluster.topology

import org.scalatest.{ GivenWhenThen, Matchers, FlatSpec }
import com.typesafe.config.ConfigFactory

/**
 *
 *
 * @author Lukasz Olczak
 */
class ClusterTopologySettingsSpec extends FlatSpec with Matchers with GivenWhenThen {

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

  "A ClusterTopologySettings" should "parse topology configuration" in {
    Given("Valid config containing topology")
    val config = ConfigFactory.parseString(topologyConfig)
    When("I parse topology config")
    val clusterTopology = ClusterTopology.fromConfig(config.getConfig("akka"))
    Then("I should get valid topology settings")
    clusterTopology shouldNot be(null)
    clusterTopology.containsZone("1") should be(true)
    clusterTopology.containsZone("2") should be(true)
    clusterTopology.containsZone("3") should be(true)
  }

}
