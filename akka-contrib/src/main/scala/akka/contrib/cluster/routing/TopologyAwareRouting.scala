package akka.contrib.cluster.routing

import com.typesafe.config.Config
import akka.actor._
import akka.contrib.cluster.topology._
import akka.cluster.Cluster
import akka.routing._
import scala.collection.immutable.IndexedSeq
import scala.collection.immutable
import akka.dispatch.Dispatchers
import akka.japi.Util._
import akka.contrib.cluster.topology.Zone
import akka.routing.SeveralRoutees
import scala.Some
import akka.routing.Router
import akka.routing.ActorRefRoutee
import akka.routing.ActorSelectionRoutee
import scala.concurrent.forkjoin.ThreadLocalRandom

case class RouteeTopology(routees: IndexedSeq[Routee], clusterTopology: ClusterTopology, selfAddress: Address) {

  val selfZone: Zone = clusterTopology.findZone(selfAddress).getOrElse(throwInvalidTopology)

  private def throwInvalidTopology = throw new IllegalArgumentException("Wrong topology. Cannot identify my zone.")

  val routeeGroupedByZone: Map[Zone, IndexedSeq[Routee]] = routees groupBy routeeZone

  val selfZoneRoutees: IndexedSeq[Routee] = {
    routeeGroupedByZone.get(selfZone) match {
      case None    ⇒ IndexedSeq.empty
      case Some(r) ⇒ r
    }
  }

  val closestRoutees: IndexedSeq[Routee] =
    if (selfZoneRoutees.isEmpty) {
      val closesNonEmptyZone = clusterTopology.proximityZones(selfZone).find(
        zone ⇒ routeeGroupedByZone.contains(zone) && !routeeGroupedByZone(zone).isEmpty)
      closesNonEmptyZone.map(routeeGroupedByZone(_)) match {
        case None    ⇒ IndexedSeq.empty
        case Some(r) ⇒ r
      }
    } else selfZoneRoutees

  protected def routeeZone(routee: Routee): Zone = {
    val address = extractNodeAddress(routee)
    clusterTopology.findZone(address).getOrElse(throwCannotAssignRoutee(routee))
  }

  private def throwCannotAssignRoutee(routee: Routee) =
    throw new IllegalArgumentException(s"Cannot assign routee: $routee to any zone")

  protected def extractNodeAddress(routee: Routee): Address = {
    val address = routee match {
      case asr: ActorSelectionRoutee ⇒ asr.selection.anchorPath.address
      case arr: ActorRefRoutee       ⇒ arr.ref.path.address
      case _                         ⇒ throw new IllegalArgumentException(s"Cannot extract address from routee")
    }
    address match {
      case Address(_, _, None, None) ⇒ selfAddress
      case a                         ⇒ a
    }
  }

}

trait TopologyAwareRoutingLogic {

  def select(message: Any, routeeTopology: RouteeTopology): IndexedSeq[Routee]

}

class TopologyAwareRoutingLogicAdapter(system: ActorSystem, adaptee: TopologyAwareRoutingLogic)
  extends RoutingLogic with NoSerializationVerificationNeeded {

  private val cluster = Cluster(system)

  protected val topology: ClusterTopology = ClusterTopology.fromConfig(system.settings.config.getConfig("akka.cluster.topology"))

  def select(message: Any, routees: IndexedSeq[Routee]): Routee = {
    val destinations = adaptee.select(message, RouteeTopology(routees, topology, cluster.selfAddress))
    SeveralRoutees(destinations)
  }

}

object TopologyAwareRoutingLogic {

  def fromConfig(config: Config, dynamicAccess: DynamicAccess): TopologyAwareRoutingLogic =
    config.getString("routing-logic") match {
      case "random-closest" ⇒ ClosestRouteeRoutingLogic
      case fqn ⇒
        val args = List(classOf[Config] -> config)
        dynamicAccess.createInstanceFor[TopologyAwareRoutingLogic](fqn, args).recover(
          {
            case exception ⇒ throw new IllegalArgumentException(
              (s"Cannot instantiate routing-logic [$fqn], " +
                "make sure it extends [akka.contrib.cluster.routing.TopologyAwareRoutingLogic] and " +
                "has constructor with [com.typesafe.config.Config] parameter"), exception)
          }).get

    }

}

object ClosestRouteeRoutingLogic extends TopologyAwareRoutingLogic {

  def chooseRandom(routees: IndexedSeq[Routee]): IndexedSeq[Routee] =
    if (routees.isEmpty) IndexedSeq.empty
    else IndexedSeq(routees(ThreadLocalRandom.current.nextInt(routees.size)))

  def select(message: Any, topology: RouteeTopology): IndexedSeq[Routee] = chooseRandom(topology.closestRoutees)

}

object SelfZoneRouteeRoutingLogic extends TopologyAwareRoutingLogic {
  def select(message: Any, topology: RouteeTopology): IndexedSeq[Routee] = topology.selfZoneRoutees
}

class TopologyAwareRoutingPool(
  topologyAwareRoutingLogic: TopologyAwareRoutingLogic,
  override val nrOfInstances: Int = 0,
  override val supervisorStrategy: SupervisorStrategy = Pool.defaultSupervisorStrategy,
  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId,
  override val usePoolDispatcher: Boolean = false)
  extends Pool {

  def this(config: Config, dynamicAccess: DynamicAccess) =
    this(
      TopologyAwareRoutingLogic.fromConfig(config, dynamicAccess),
      config.getInt("nr-of-instances"),
      Pool.defaultSupervisorStrategy,
      Dispatchers.DefaultDispatcherId,
      config.hasPath("pool-dispatcher"))

  def createRouter(system: ActorSystem): Router =
    new Router(new TopologyAwareRoutingLogicAdapter(system, topologyAwareRoutingLogic))

  def resizer: Option[Resizer] = None

}

final case class TopologyAwareRoutingGroup(
  topologyAwareRoutingLogic: TopologyAwareRoutingLogic,
  override val paths: immutable.Iterable[String] = Nil,
  override val routerDispatcher: String = Dispatchers.DefaultDispatcherId)
  extends Group {

  def this(config: Config, dynamicAccess: DynamicAccess) =
    this(
      TopologyAwareRoutingLogic.fromConfig(config, dynamicAccess),
      immutableSeq(config.getStringList("routees.paths")))

  def createRouter(system: ActorSystem): Router =
    new Router(new TopologyAwareRoutingLogicAdapter(system, topologyAwareRoutingLogic))

}

class ReplicationRoutingLogic(globalReplicationFactor: Int, zoneReplicationFactor: Map[String, Int])
  extends TopologyAwareRoutingLogic {

  def select(message: Any, routeeTopology: RouteeTopology): IndexedSeq[Routee] = ???

}
