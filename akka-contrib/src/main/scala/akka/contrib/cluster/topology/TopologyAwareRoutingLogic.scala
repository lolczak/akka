package akka.contrib.cluster.topology

import akka.routing._
import scala.collection.immutable.IndexedSeq
import akka.actor.{ ActorSystem, Address }
import akka.routing.ActorRefRoutee
import akka.routing.ActorSelectionRoutee
import akka.cluster.Cluster

/**
 *
 *
 * @author Lukasz Olczak
 */
abstract class TopologyAwareRoutingLogic(cluster: Cluster) extends RoutingLogic {

  protected val topology: ClusterTopology = ClusterTopologySettings.fromConfig(cluster.system.settings.config)

  protected val zones: IndexedSeq[Zone] = topology.values.toIndexedSeq

  protected val myZone: Zone = {
    val myZoneOption = zones.find(_.contains(cluster.selfAddress))
    if (myZoneOption.isEmpty) throw new IllegalArgumentException("Wrong topology. Cannot identify my zone.")
    myZoneOption.get
  }

  /**
   * @inheritdoc
   */
  def select(message: Any, routees: IndexedSeq[Routee]): Routee = {
    val routeesTopology: Map[Zone, IndexedSeq[Routee]] = routees groupBy routeeZone
    val destinations = for (
      zone ← selectZones(message, myZone, topology);
      zoneRoutees = routeesTopology.get(zone).get;
      routees ← selectRoutees(message, zone, zoneRoutees)
    ) yield routees
    SeveralRoutees(destinations)
  }

  protected def routeeZone(routee: Routee): Zone = {
    val address = extractNodeAddress(routee)
    val zoneOption = zones.find(_.contains(address))
    if (zoneOption.isEmpty) throw new IllegalArgumentException(s"Cannot assign routee: $routee to any zone")
    zoneOption.get
  }

  protected def extractNodeAddress(routee: Routee): Address = {
    routee match {
      case asr: ActorSelectionRoutee ⇒ asr.selection.anchorPath.address
      case arr: ActorRefRoutee       ⇒ arr.ref.path.address
      case _                         ⇒ throw new IllegalArgumentException(s"Cannot extract address from routee")
    }
  }

  protected def selectZones(message: Any, myZone: Zone, topology: ClusterTopology): IndexedSeq[Zone]

  protected def selectRoutees(message: Any, zone: Zone, routees: IndexedSeq[Routee]): IndexedSeq[Routee]

}
