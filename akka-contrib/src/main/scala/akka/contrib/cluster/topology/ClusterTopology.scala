package akka.contrib.cluster.topology

import com.typesafe.config.Config
import akka.japi.Util._
import scala.collection.immutable.IndexedSeq

/**
 *
 *
 * @author Lukasz Olczak
 */

class ClusterTopology(val topology: Map[String, Zone]) {

  val zones: IndexedSeq[Zone] = topology.values.toIndexedSeq

  def getClosestZoneFor(zone:Zone) = topology.get(zone.proximity(0)).get

}


object ClusterTopology {

  def fromConfig(config: Config): ClusterTopology = {
    val zonesConfig = immutableSeq(config.getConfigList("cluster.topology.zones"))
    val zones = for (zoneConfig ← zonesConfig)
      yield Zone(
      id = zoneConfig.getString("id"),
      addressClassifier = AddressClassifier.fromString(zoneConfig.getString("zone-classifier")),
      proximity = immutableSeq(zoneConfig.getStringList("proximity-list")))

    val topology = zones.map(zone ⇒ zone.id -> zone).toMap
    new ClusterTopology(topology)
  }

}

