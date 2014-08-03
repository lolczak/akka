package akka.contrib.cluster.topology

import com.typesafe.config.Config
import akka.japi.Util._
import scala.collection.immutable.IndexedSeq
import akka.actor.Address

/**
 *
 *
 * @author Lukasz Olczak
 */
class ClusterTopology(private val topology: Map[String, Zone]) {

  val zones: IndexedSeq[Zone] = topology.values.toIndexedSeq

  def getClosestZoneFor(zone: Zone): Zone = topology.get(zone.proximity(0)).get

  def findZone(zoneId: String): Option[Zone] = topology.get(zoneId)

  def getProximityZonesFor(selfZone: Zone): Seq[Zone] = for (zoneId ← selfZone.proximity; zone ← findZone(zoneId)) yield zone

  def containsZone(zoneId: String) = topology.contains(zoneId)

  def findZone(predicate: Zone ⇒ Boolean): Option[Zone] = topology.values.find(predicate)

  def findZone(nodeAddress: Address): Option[Zone] = findZone(_.contains(nodeAddress))

}

object ClusterTopology {

  def fromConfig(config: Config): ClusterTopology = {
    val zonesConfig = immutableSeq(config.getConfigList("zones"))
    val zones = for (zoneConfig ← zonesConfig)
      yield Zone(
      id = zoneConfig.getString("id"),
      addressClassifier = AddressClassifier.fromString(zoneConfig.getString("zone-classifier")),
      proximity = immutableSeq(zoneConfig.getStringList("proximity-list")))

    val topology = zones.map(zone ⇒ zone.id -> zone).toMap
    new ClusterTopology(topology)
  }

}

case class Zone(id: String, addressClassifier: AddressClassifier, proximity: Seq[String]) {

  def contains(address: Address): Boolean = addressClassifier(address)

}

