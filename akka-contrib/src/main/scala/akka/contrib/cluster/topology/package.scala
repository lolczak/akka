package akka.contrib.cluster

import akka.actor.Address

/**
 *
 *
 * @author Lukasz Olczak
 */
package object topology {

  type AddressClassifier = Address ⇒ Boolean

}
