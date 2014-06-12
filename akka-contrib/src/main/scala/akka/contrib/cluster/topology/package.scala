package akka.contrib.cluster

import akka.actor.Address

/**
 *
 *
 * @author Lukasz Olczak
 */
package object topology {

  type ClusterTopology = Map[String, Zone]

  type AddressClassifier = Address ⇒ Boolean

}
