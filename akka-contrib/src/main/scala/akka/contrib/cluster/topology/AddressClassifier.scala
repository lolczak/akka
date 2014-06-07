package akka.contrib.cluster.topology

import akka.actor.Address
import org.apache.commons.net.util.SubnetUtils

/**
 *
 *
 * @author Lukasz Olczak
 */
object AddressClassifier {

  val NetMaskPrefix = "net:"

  val RegExPrefix = "reg-ex:"

  def fromString(pattern: String): AddressClassifier = {
    pattern match {
      case netMakPattern if netMakPattern.startsWith(NetMaskPrefix) => NetMaskClassifier(pattern.substring(NetMaskPrefix.length))
      case regExPattern if regExPattern.startsWith(RegExPrefix) => RegExClassifier(pattern.substring(RegExPrefix.length))
      case _ => throw new IllegalArgumentException(s"address pattern $pattern is not valid")
    }
  }

}

case class NetMaskClassifier(pattern: String) extends AddressClassifier {

  private val cidrNotation = new SubnetUtils(pattern).getInfo

  def apply(address: Address): Boolean = cidrNotation.isInRange(address.host.getOrElse(throw new IllegalArgumentException()))

}

case class RegExClassifier(pattern: String) extends AddressClassifier {

  def apply(address: Address): Boolean = ???

}
