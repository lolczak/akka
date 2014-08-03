package akka.contrib.cluster.topology

import akka.actor.Address
import org.apache.commons.net.util.SubnetUtils
import akka.ConfigurationException
import scala.util.{Failure, Success, Try}

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
      case _                       ⇒ throw new ConfigurationException(s"Address classifier [$pattern] is invalid")
    }
  }

  def throwHostMissing = throw new IllegalArgumentException("Cannot classify address: host missing")

}

case class NetMaskClassifier(pattern: String) extends AddressClassifier {

  import AddressClassifier._

  private val cidrPattern = Try(new SubnetUtils(pattern).getInfo) match {
    case Success(cidrPattern) => cidrPattern
    case Failure(cause) => throw new ConfigurationException(s"Address classifier [$pattern] is invalid", cause)
  }

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
