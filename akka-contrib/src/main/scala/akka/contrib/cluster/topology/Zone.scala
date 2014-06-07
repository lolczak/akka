package akka.contrib.cluster.topology

import akka.actor.Address

/**
 *
 *
 * @author Lukasz Olczak
 */
case class Zone(id: String, addressClassifier: AddressClassifier, proximity: List[String]) {

  def contains(address: Address): Boolean = addressClassifier(address)

}
