package akka.contrib.cluster.topology

import akka.routing.{ Routee, RoundRobinRoutingLogic, RoutingLogic }
import akka.cluster.Cluster
import scala.collection.immutable.IndexedSeq

/**
 *
 *
 * @author Lukasz Olczak
 */
class MyZoneRoutingLogic(cluster: Cluster, private val insideZoneRoutingLogic: RoutingLogic = new RoundRobinRoutingLogic)
  extends TopologyAwareRoutingLogic(cluster) {

  protected def selectRoutees(message: Any, zone: Zone, routees: IndexedSeq[Routee]): IndexedSeq[Routee] =
    IndexedSeq(insideZoneRoutingLogic.select(message, routees))

  protected def selectZones(message: Any, myZone: Zone, topology: ClusterTopology): IndexedSeq[Zone] = IndexedSeq(myZone)

}