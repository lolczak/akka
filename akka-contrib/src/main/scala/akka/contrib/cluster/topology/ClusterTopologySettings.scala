package akka.contrib.cluster.topology

import com.typesafe.config.Config
import akka.japi.Util._

/**
 *
 *
 * @author Lukasz Olczak
 */
object ClusterTopologySettings {

  def fromConfig(config: Config): ClusterTopologySettings = {
    val zonesConfig = immutableSeq(config.getConfigList("cluster.topology.zones"))
    val zones = for (zoneConfig <- zonesConfig)
    yield ZoneSettings(
        id = zoneConfig.getString("id"),
        nodeClassifier = zoneConfig.getString("node-classifier"),
        proximity = immutableSeq(zoneConfig.getStringList("proximity-list")))
    ClusterTopologySettings(zones)
  }

}


case class ClusterTopologySettings(zones: Seq[ZoneSettings])

case class ZoneSettings(id: String, nodeClassifier: String, proximity: Seq[String])
