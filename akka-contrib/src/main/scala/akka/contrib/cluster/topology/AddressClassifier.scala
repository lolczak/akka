package akka.contrib.cluster.topology

import akka.actor.Address
import org.apache.commons.net.util.SubnetUtils

/**
 *
 *
 * @author Lukasz Olczak
 */
object AddressClassifier {

  val NetMaskPattern = """^net:([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/[0-9]{1,2})$""".r

  val RegExPattern = """^reg-ex:(.+)$""".r

  def fromString(pattern: String): AddressClassifier = {
    pattern match {
      case NetMaskPattern(netmask) ⇒ NetMaskClassifier(netmask)
      case RegExPattern(regEx)     ⇒ RegExClassifier(regEx)
      case _                       ⇒ throw new IllegalArgumentException(s"address pattern $pattern is not valid")
    }
  }

  def throwHostMissing = throw new IllegalArgumentException("Cannot classify address: host missing")

}

case class NetMaskClassifier(pattern: String) extends AddressClassifier {

  import AddressClassifier._

  private val cidrPattern = new SubnetUtils(pattern).getInfo

  def apply(address: Address): Boolean = cidrPattern.isInRange(address.host.getOrElse(throwHostMissing))

}

case class RegExClassifier(pattern: String) extends AddressClassifier {

  import AddressClassifier._

  private val regExPattern = pattern.r

  def apply(address: Address): Boolean = address.host.getOrElse(throwHostMissing) match {
    case regExPattern() ⇒ true
    case _              ⇒ false
  }

}
