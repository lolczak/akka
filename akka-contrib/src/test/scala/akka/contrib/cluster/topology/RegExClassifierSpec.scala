package akka.contrib.cluster.topology

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.prop.Tables.Table
import org.scalatest.prop.TableDrivenPropertyChecks._
import akka.actor.Address

/**
 *
 *
 * @author Lukasz Olczak
 */
class RegExClassifierSpec extends WordSpec with Matchers {
  val matchingAddresses = Table(
    ("reg-ex:[a-z]*dev\\.domain\\.org", "node1.dev.domain.org"),
    ("reg-ex:.*\\.org", "some.domain.org"),
    ("reg-ex:node[0-9]\\.domain\\.org", "node1.domain.org")
  )

  val nonMatchingAddresses = Table(
    ("reg-ex:[a-z]*dev\\.domain\\.org", "node1.dev.domain.org.pl"),
    ("reg-ex:.*\\.org", "some.domain.com"),
    ("reg-ex:node[0-9]\\.domain\\.org", "node11.domain.org")
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
