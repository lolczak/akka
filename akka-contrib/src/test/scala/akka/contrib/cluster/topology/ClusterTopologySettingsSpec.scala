package akka.contrib.cluster.topology

import org.scalatest.{GivenWhenThen, Matchers, FlatSpec}
import com.typesafe.config.ConfigFactory

/**
 *
 *
 * @author Lukasz Olczak
 */
class ClusterTopologySettingsSpec extends FlatSpec with Matchers with GivenWhenThen {

  "A ClusterTopologySettings" should "parse topology configuration" in {
    Given("Valid config containing topology")
    val config = ConfigFactory.parseURL(Thread.currentThread().getContextClassLoader().getResource("topology.conf"))
    When("I parse topology config")
    val clusterTopologySettings = ClusterTopologySettings.fromConfig(config.getConfig("akka"))
    Then("I should get valid topology settings")
    clusterTopologySettings shouldNot be(null)
  }

}
