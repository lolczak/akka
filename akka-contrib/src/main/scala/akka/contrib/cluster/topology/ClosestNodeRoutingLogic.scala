package akka.contrib.cluster.topology

import akka.routing.{ActorSelectionRoutee, Routee, RoutingLogic}
import scala.collection.immutable.IndexedSeq
import akka.actor.{ActorPath, ActorRef}

/**
 *
 *
 * @author Lukasz Olczak
 */
class ClosestNodeRoutingLogic(private val insideZoneRoutingLogic: RoutingLogic) {

}
