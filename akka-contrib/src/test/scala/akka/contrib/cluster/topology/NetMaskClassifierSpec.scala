package akka.contrib.cluster.topology

import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.{Matchers, WordSpec}
import akka.actor.Address

/**
 *
 *
 * @author Lukasz Olczak
 */
class NetMaskClassifierSpec extends WordSpec with Matchers {


  val matchingAddresses = Table(
    ("net:192.168.0.0/16", "192.168.1.1"),
    ("net:192.168.0.0/16", "192.168.1.2"),
    ("net:192.168.0.0/8", "192.167.1.1"),
    ("net:192.168.0.0/24", "192.168.0.1"),
    ("net:192.168.0.0/1", "193.168.1.1"),
    ("net:192.168.0.0/1", "213.168.1.1"),
    ("net:192.168.0.0/1", "221.168.1.1"),
    ("net:192.168.0.0/1", "199.168.1.1")
  )

  val nonMatchingAddresses = Table(
    ("net:192.168.0.0/16", "192.161.1.1"),
    ("net:192.168.0.0/16", "92.168.1.2"),
    ("net:192.168.0.0/8", "122.167.1.1"),
    ("net:192.168.0.0/24", "192.168.1.1")
  )

  "A NetMaskClassifier " must {

    "return true if the address is in the range of the subnet" in {
      forAll(matchingAddresses) {
        (pattern, host) =>
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier(Address("akka.tcp", "test", host, 123)) should be(true)
      }
    }

    "return false if the address is not in the range of the subnet" in {
      forAll(nonMatchingAddresses) {
        (pattern, host) =>
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier(Address("akka.tcp", "test", host, 123)) should be(false)
      }
    }

  }

}
