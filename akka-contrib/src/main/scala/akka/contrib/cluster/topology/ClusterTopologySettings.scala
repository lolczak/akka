package akka.contrib.cluster.topology

import com.typesafe.config.Config
import akka.japi.Util._

/**
 *
 *
 * @author Lukasz Olczak
 */
object ClusterTopologySettings {

  def fromConfig(config: Config): ClusterTopology = {
    val zonesConfig = immutableSeq(config.getConfigList("cluster.topology.zones"))
    val zones = for (zoneConfig ← zonesConfig)
      yield Zone(
      id = zoneConfig.getString("id"),
      addressClassifier = AddressClassifier.fromString(zoneConfig.getString("zone-classifier")),
      proximity = immutableSeq(zoneConfig.getStringList("proximity-list")))

    zones.map(zone ⇒ zone.id -> zone).toMap
  }

}

