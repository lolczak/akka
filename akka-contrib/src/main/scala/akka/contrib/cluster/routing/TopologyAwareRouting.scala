package akka.contrib.cluster.routing

import com.typesafe.config.Config
import akka.actor.{ SupervisorStrategy, ActorSystem, Address, DynamicAccess }
import akka.contrib.cluster.topology._
import akka.cluster.Cluster
import akka.routing._
import scala.collection.immutable.IndexedSeq
import scala.collection.immutable
import akka.dispatch.Dispatchers
import akka.japi.Util._
import akka.contrib.cluster.topology.Zone
import akka.routing.SeveralRoutees
import akka.routing.Router
import akka.routing.ActorRefRoutee
import akka.routing.ActorSelectionRoutee

class TopologyAwareRoutingLogic(system: ActorSystem, zoneRoutingLogic: ZoneRoutingLogic,
                                nodeRoutingLogic: NodeRoutingLogic) extends RoutingLogic {

  private val cluster = Cluster(system)

  protected val topology: ClusterTopology = ClusterTopology.fromConfig(system.settings.config.getConfig("akka"))

  protected val myZone: Zone = {
    val myZoneOption = topology.zones.find(_.contains(cluster.selfAddress)) //todo refactor
    if (myZoneOption.isEmpty) throw new IllegalArgumentException("Wrong topology. Cannot identify my zone.")
    myZoneOption.get
  }

  def select(message: Any, routees: IndexedSeq[Routee]): Routee = {
    println(s"routees size ${routees.size}")
    for (routee ← routees) {
      routee match {
        case asr: ActorSelectionRoutee ⇒ println(s"Routee: ${asr.selection.anchorPath} ${routee.getClass}")
        case arr: ActorRefRoutee       ⇒ println(s"Routee: ${arr.ref.path} ${routee.getClass}")
        case _                         ⇒ throw new IllegalArgumentException(s"Cannot extract address from routee")
      }
    }

    val routeesTopology: Map[Zone, IndexedSeq[Routee]] = routees groupBy routeeZone
    val destinations = for (
      zone ← zoneRoutingLogic.selectZones(message, myZone, topology);
      zoneRoutees = routeesTopology.get(zone).get;
      routees ← nodeRoutingLogic.selectRoutees(message, zone, zoneRoutees)
    ) yield routees
    SeveralRoutees(destinations)
  }

  protected def routeeZone(routee: Routee): Zone = {
    val address = extractNodeAddress(routee)
    val zoneOption = topology.zones.find(_.contains(address)) //todo refactor
    if (zoneOption.isEmpty) throw new IllegalArgumentException(s"Cannot assign routee: $routee to any zone")
    zoneOption.get
  }

  protected def extractNodeAddress(routee: Routee): Address = {
    val address = routee match {
      case asr: ActorSelectionRoutee ⇒ asr.selection.anchorPath.address
      case arr: ActorRefRoutee       ⇒ arr.ref.path.address
      case _                         ⇒ throw new IllegalArgumentException(s"Cannot extract address from routee")
    }
    address match {
      case Address(_, _, None, None) ⇒ cluster.selfAddress
      case a                         ⇒ a
    }
  }

}

trait ZoneRoutingLogic {
  def selectZones(message: Any, myZone: Zone, topology: ClusterTopology): IndexedSeq[Zone]
}

object ZoneRoutingLogic {

  def fromConfig(config: Config, dynamicAccess: DynamicAccess): ZoneRoutingLogic =
    config.getString("zone-routing-logic") match {
      case "closest-zone" ⇒ ClosestZoneRoutingLogic
      case "my-zone"      ⇒ MyZoneRoutingLogic
      case fqn ⇒
        val args = List(classOf[Config] -> config)
        dynamicAccess.createInstanceFor[ZoneRoutingLogic](fqn, args).recover(
          {
            case exception ⇒ throw new IllegalArgumentException(
              (s"Cannot instantiate routing-logic [$fqn], " +
                "make sure it extends [akka.contrib.cluster.routing.ZoneRoutingLogic] and " +
                "has constructor with [com.typesafe.config.Config] parameter"), exception)
          }).get

    }

}

object MyZoneRoutingLogic extends ZoneRoutingLogic {
  def selectZones(message: Any, myZone: Zone, topology: ClusterTopology): IndexedSeq[Zone] = IndexedSeq(myZone)
}

object ClosestZoneRoutingLogic extends ZoneRoutingLogic {
  def selectZones(message: Any, myZone: Zone, topology: ClusterTopology): IndexedSeq[Zone] =
    IndexedSeq(topology.getClosestZoneFor(myZone))
}

trait NodeRoutingLogic {
  def selectRoutees(message: Any, zone: Zone, routees: IndexedSeq[Routee]): IndexedSeq[Routee]
}

private class NodeRoutingLogicAdapter(val adaptee: RoutingLogic) extends NodeRoutingLogic {
  def selectRoutees(message: Any, zone: Zone, routees: IndexedSeq[Routee]): IndexedSeq[Routee] =
    IndexedSeq(adaptee.select(message, routees))
}

object NodeRoutingLogic {
  def fromConfig(config: Config, dynamicAccess: DynamicAccess): NodeRoutingLogic =
    config.getString("node-routing-logic") match {
      //      case "round-robin" => MyZoneRoutingLogic
      case "random"           ⇒ new NodeRoutingLogicAdapter(RandomRoutingLogic())
      case "broadcast"        ⇒ new NodeRoutingLogicAdapter(BroadcastRoutingLogic())
      case "smallest-mailbox" ⇒ new NodeRoutingLogicAdapter(SmallestMailboxRoutingLogic())
      case fqn ⇒
        val args = List(classOf[Config] -> config)
        dynamicAccess.createInstanceFor[NodeRoutingLogic](fqn, args).recover(
          {
            case exception ⇒ throw new IllegalArgumentException(
              (s"Cannot instantiate routing-logic [$fqn], " +
                "make sure it extends [akka.contrib.cluster.routing.ZoneRoutingLogic] and " +
                "has constructor with [com.typesafe.config.Config] parameter"), exception)
          }).get

    }
}

class TopologyAwareRoutingPool(
  zoneRoutingLogic: ZoneRoutingLogic, nodeRoutingLogic: NodeRoutingLogic,
  override val nrOfInstances: Int = 0,
  override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
  override val usePoolDispatcher: Boolean = false)

  extends Pool {

  def this(config: Config, dynamicAccess: DynamicAccess) =
    this(
      ZoneRoutingLogic.fromConfig(config, dynamicAccess),
      NodeRoutingLogic.fromConfig(config, dynamicAccess),
      config.getInt("nr-of-instances"),
      Pool.defaultSupervisorStrategy,
      Dispatchers.DefaultDispatcherId,
      config.hasPath("pool-dispatcher"))

  def createRouter(system: ActorSystem): Router =
    new Router(new TopologyAwareRoutingLogic(system, zoneRoutingLogic, nodeRoutingLogic))

  def resizer: Option[Resizer] = None

}

final case class TopologyAwareRoutingGroup(
  zoneRoutingLogic: ZoneRoutingLogic, nodeRoutingLogic: NodeRoutingLogic,
  override val paths: immutable.Iterable[String] = Nil,
  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
  extends Group {

  def this(config: Config, dynamicAccess: DynamicAccess) =
    this(
      ZoneRoutingLogic.fromConfig(config, dynamicAccess),
      NodeRoutingLogic.fromConfig(config, dynamicAccess),
      immutableSeq(config.getStringList("routees.paths")))

  def createRouter(system: ActorSystem): Router =
    new Router(new TopologyAwareRoutingLogic(system, zoneRoutingLogic, nodeRoutingLogic))

}
