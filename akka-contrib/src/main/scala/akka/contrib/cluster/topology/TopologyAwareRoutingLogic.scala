package akka.contrib.cluster.topology

import akka.routing._
import scala.collection.immutable.IndexedSeq
import akka.actor.Address
import akka.routing.ActorRefRoutee
import akka.routing.ActorSelectionRoutee

/**
 *
 *
 * @author Lukasz Olczak
 */
trait TopologyAwareRoutingLogic extends RoutingLogic {

  val myZone: Zone

  val zones: IndexedSeq[Zone]

  /**
   * Pick the destination for a given message. Normally it picks one of the
   * passed `routees`, but in the end it is up to the implementation to
   * return whatever [[akka.routing.Routee]] to use for sending a specific message.
   *
   * When implemented from Java it can be good to know that
   * `routees.apply(index)` can be used to get an element
   * from the `IndexedSeq`.
   */
  def select(message: Any, routees: IndexedSeq[Routee]): Routee = {
    val topology: Map[Zone, IndexedSeq[Routee]] = routees groupBy routeeZone
    val destinations = for (
      zone <- selectZones(message, myZone, zones);
      zoneRoutees = topology.get(zone).get;
      routees <- selectRoutees(message, zone, zoneRoutees)
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
      case asr: ActorSelectionRoutee => asr.selection.anchorPath.address
      case arr: ActorRefRoutee => arr.ref.path.address
      case _ => throw new IllegalArgumentException(s"Cannot extract address from routee")
    }
  }

  protected def selectZones(message: Any, myZone: Zone, availableZones: IndexedSeq[Zone]): IndexedSeq[Zone]

  protected def selectRoutees(message: Any, zone: Zone, routees: IndexedSeq[Routee]): IndexedSeq[Routee]

}
