package akka.contrib.cluster.routing

import com.typesafe.config.Config
import akka.actor._
import akka.contrib.cluster.topology._
import akka.cluster.Cluster
import akka.routing._
import scala.collection.immutable.IndexedSeq
import scala.collection.{ TraversableLike, immutable }
import akka.dispatch.Dispatchers
import akka.japi.Util._
import scala.concurrent.forkjoin.ThreadLocalRandom
import scala.util.Random
import akka.contrib.cluster.topology.Zone
import akka.routing.SeveralRoutees
import scala.Some
import akka.routing.Router
import akka.routing.ActorRefRoutee
import akka.routing.ActorSelectionRoutee
import scala.collection.mutable.ListBuffer
import TopologyAwareRoutingImplicits._
import scala.collection.generic.CanBuildFrom

trait TopologyAwareRoutingLogic {

  def select(message: Any, routees: TopologyAwareRoutees): IndexedSeq[Routee]

}

trait TopologyAwareRoutees {

  def isEmpty: Boolean

  def size: Int

  def fromZone(zoneId: String): IndexedSeq[Routee]

  def fromSelfZone: IndexedSeq[Routee]

  def fromClosestZone: IndexedSeq[Routee]

  def asIndexedSeq: IndexedSeq[Routee]

  def asMap: Map[Zone, IndexedSeq[Routee]]

}

private[cluster] class TopologyAwareRouteesImpl(
  routees: IndexedSeq[Routee],
  clusterTopology: ClusterTopology,
  selfAddress: Address)
  extends TopologyAwareRoutees {

  private val selfZone: Zone = clusterTopology.findZone(selfAddress).getOrElse(throwInvalidTopology)

  private def throwInvalidTopology = throw new IllegalArgumentException("Cluster topology is invalid - cannot identify a self zone.")

  val asIndexedSeq: IndexedSeq[Routee] = IndexedSeq(routees: _*)

  val isEmpty: Boolean = routees.isEmpty

  val size: Int = routees.size

  val asMap: Map[Zone, IndexedSeq[Routee]] = routees groupBy routeeZone

  def fromZone(zoneId: String): IndexedSeq[Routee] = {
    val zone = clusterTopology.getZone(zoneId)
    zone.flatMap(asMap.get(_)).getOrElse(IndexedSeq.empty)
  }

  val fromSelfZone: IndexedSeq[Routee] = {
    asMap.get(selfZone) match {
      case None    ⇒ IndexedSeq.empty
      case Some(r) ⇒ r
    }
  }

  val fromClosestZone: IndexedSeq[Routee] =
    if (fromSelfZone.isEmpty) {
      //todo tailrec or collectFirst
      val closesNonEmptyZone = clusterTopology.proximityZones(selfZone).find(
        zone ⇒ asMap.contains(zone) && !asMap(zone).isEmpty)
      closesNonEmptyZone.map(asMap(_)) match {
        case None    ⇒ IndexedSeq.empty
        case Some(r) ⇒ r
      }
    } else fromSelfZone

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

private class TopologyAwareRoutingLogicAdapter(system: ActorSystem, adaptee: TopologyAwareRoutingLogic)
  extends RoutingLogic with NoSerializationVerificationNeeded {

  private val cluster = Cluster(system)

  protected val clusterTopology: ClusterTopology =
    ClusterTopology.fromConfig(system.settings.config.getConfig("akka.cluster.topology"))

  def select(message: Any, routees: IndexedSeq[Routee]): Routee = {
    val destinations = adaptee.select(message, new TopologyAwareRouteesImpl(routees, clusterTopology, cluster.selfAddress))
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

  def select(message: Any, routees: TopologyAwareRoutees): IndexedSeq[Routee] =
    if (routees.isEmpty) IndexedSeq.empty
    else routees.fromClosestZone draw 1

}

object SelfZoneRouteeRoutingLogic extends TopologyAwareRoutingLogic {

  def select(message: Any, routees: TopologyAwareRoutees): IndexedSeq[Routee] = routees fromSelfZone

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

/**
 * Constraint: replication-factor-zone <= nodes-in-that-zone
 *
 * @param globalReplicationFactor
 * @param zoneReplicationFactors
 */
case class ReplicationRoutingLogic(
  private val globalReplicationFactor: Int,
  private val zoneReplicationFactors: Option[Map[String, Int]])
  extends TopologyAwareRoutingLogic {

  private val sumOfZoneReplicationFactors = zoneReplicationFactors.map(_.values.reduce(_+_)).getOrElse(globalReplicationFactor)

  if (globalReplicationFactor <= 0) throw new IllegalArgumentException("Global replication factor must be > 0")

  if (globalReplicationFactor != sumOfZoneReplicationFactors)
    throw new IllegalArgumentException(s"Global replication factor [$globalReplicationFactor] must be equal to " +
      s"sum of zone replication factors [$sumOfZoneReplicationFactors]")

  def select(message: Any, routees: TopologyAwareRoutees): IndexedSeq[Routee] = {
    if (routees.size < globalReplicationFactor)
      throw new IllegalArgumentException("Number of routees [${routeeTopology.allRoutees.size}] is lesser than " +
        "global replication factor [$globalReplicationFactor]")

    zoneReplicationFactors match {
      case None          ⇒ selectWithGlobalConstraint(routees)
      case Some(factors) ⇒ selectWithZoneConstraints(routees)
    }
  }

  protected def selectWithGlobalConstraint(routees: TopologyAwareRoutees): IndexedSeq[Routee] =
    routees.asIndexedSeq draw globalReplicationFactor

  protected def selectWithZoneConstraints(routees: TopologyAwareRoutees): IndexedSeq[Routee] = {
    val result = new ListBuffer[Routee]
    for ((zoneId, zoneReplicationFactor) ← zoneReplicationFactors.get) {
      val zoneRoutees = routees fromZone zoneId
      if (zoneRoutees.size < zoneReplicationFactor)
        throw new IllegalArgumentException(
          s"Number of zone routees [${zoneRoutees.size}}] is lesser than zone [$zoneId] " +
            s"replication factor [$zoneReplicationFactor]")

      val selected = zoneRoutees draw zoneReplicationFactor
      result appendAll selected
    }
    result.toIndexedSeq
  }

}

private object TopologyAwareRoutingImplicits {

  implicit def wrapCollection[T, CC[X] <: TraversableLike[X, CC[X]]](xs: CC[T])(implicit bf: CanBuildFrom[CC[T], T, CC[T]]): CollectionHelper[T, CC] =
    new CollectionHelper[T, CC](xs)

}

private class CollectionHelper[T, CC[X] <: TraversableLike[X, CC[X]]](xs: CC[T])(implicit bf: CanBuildFrom[CC[T], T, CC[T]]) {

  def draw(count: Int): CC[T] = {
    require(!xs.isEmpty)
    require(count >= 0)
    require(count <= xs.size)
    val shuffled = new Random(ThreadLocalRandom.current).shuffle(xs)
    shuffled.take(count)
  }

}
