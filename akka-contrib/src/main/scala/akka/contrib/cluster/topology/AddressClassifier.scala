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
      case NetMaskPattern(netmask) => NetMaskClassifier(netmask)
      case RegExPattern(regEx) => RegExClassifier(regEx)
      case _ => throw new IllegalArgumentException(s"address pattern $pattern is not valid")
    }
  }

}

case class NetMaskClassifier(pattern: String) extends AddressClassifier {

  private val cidrPattern = new SubnetUtils(pattern).getInfo

  def apply(address: Address): Boolean = cidrPattern.isInRange(address.host.getOrElse(throw new IllegalArgumentException()))

}

case class RegExClassifier(pattern: String) extends AddressClassifier {
  
  private val regExPattern = pattern.r

  def apply(address: Address): Boolean = address.host.getOrElse(throw new IllegalArgumentException()) match {
    case regExPattern() => true
    case _ => false
  }

}
