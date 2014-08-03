package akka.contrib.cluster.topology

import org.scalatest.prop.TableDrivenPropertyChecks._
import akka.ConfigurationException
import akka.testkit.AkkaSpec
import akka.actor.Address


/**
 *
 *
 * @author Lukasz Olczak
 */
object AddressClassifierSpec {

  val InvalidPatterns = Table(
    "",
    "dns:dgsdg",
    "network:dsgsdgs",
    "NET:192.168.0.0/24",
    "net:abc.168.0.0/24",
    "net:192.168.0.0",
    "net:0.0.0.0/0",
    "regEx:edfrgrbr",
    "REG-EX:edfrgrbr",
    "192.168.0.0/24",
    "net:192.168.0.0/16net:",
    "[a-z]*dev.some.org")

  val ValidNetMaskPatterns = Table(
    "net:192.168.0.0/16",
    "net:192.168.0.0/16",
    "net:192.168.0.0/8",
    "net:192.168.0.0/24",
    "net:1.0.0.0/1",
    "net:192.168.0.0/1")

  val ValidRegExPatterns = Table(
    "reg-ex:[a-z]*dev\\.domain\\.org",
    "reg-ex:[a-z]*\\.1\\.datacenter1\\.domain\\.org",
    "reg-ex:[a-z]*dev\\.some\\.org")

  val NetMaskMatchingAddresses = Table(
    ("net:192.168.0.0/16", "192.168.1.1"),
    ("net:192.168.0.0/16", "192.168.1.2"),
    ("net:192.168.0.0/8", "192.167.1.1"),
    ("net:192.168.0.0/24", "192.168.0.1"),
    ("net:192.168.0.0/1", "193.168.1.1"),
    ("net:192.168.0.0/1", "213.168.1.1"),
    ("net:192.168.0.0/1", "221.168.1.1"),
    ("net:192.168.0.0/1", "199.168.1.1"))

  val NetMaskNonMatchingAddresses = Table(
    ("net:192.168.0.0/16", "192.161.1.1"),
    ("net:192.168.0.0/16", "92.168.1.2"),
    ("net:192.168.0.0/8", "122.167.1.1"),
    ("net:192.168.0.0/24", "192.168.1.1"))

  val RegExMatchingAddresses = Table(
    ("reg-ex:[a-z]*dev\\.domain\\.org", "node1.dev.domain.org"),
    ("reg-ex:.*\\.org", "some.domain.org"),
    ("reg-ex:node[0-9]\\.domain\\.org", "node1.domain.org"))

  val RegExNonMatchingAddresses = Table(
    ("reg-ex:[a-z]*dev\\.domain\\.org", "node1.dev.domain.org.pl"),
    ("reg-ex:.*\\.org", "some.domain.com"),
    ("reg-ex:node[0-9]\\.domain\\.org", "node11.domain.org"))

  val TestProtocol = "akka.tcp"

  val TestSystem = "test"

  val TestPort = 1234

}

class AddressClassifierSpec extends AkkaSpec {

  import AddressClassifierSpec._

  "An AddressClassifier" must {

    "throw exception upon malformed address pattern" in {
      forAll(InvalidPatterns) {
        pattern ⇒
          intercept[ConfigurationException] {
            AddressClassifier.fromString(pattern)
          }
      }
    }

    "be able to parse a netmask pattern" in {
      forAll(ValidNetMaskPatterns) {
        pattern ⇒
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier shouldNot be(null)
      }
    }

    "be able to parse a reg ex pattern" in {
      forAll(ValidRegExPatterns) {
        pattern ⇒
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier shouldNot be(null)
      }
    }

  }

  "A NetMaskClassifier " must {

    "classify addresses in the range of the subnet" in {
      forAll(NetMaskMatchingAddresses) {
        (pattern, host) ⇒
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier(Address(TestProtocol, TestSystem, host, TestPort)) should be(true)
      }
    }

    "not classify addresses which are not in the range of the subnet" in {
      forAll(NetMaskNonMatchingAddresses) {
        (pattern, host) ⇒
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier(Address(TestProtocol, TestSystem, host, TestPort)) should be(false)
      }
    }

  }

  "A RegExClassifier " must {

    "classify addresses matching against DNS pattern" in {
      forAll(RegExMatchingAddresses) {
        (pattern, host) ⇒
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier(Address(TestProtocol, TestSystem, host, TestPort)) should be(true)
      }
    }

    "not classify addresses which not match against DNS pattern" in {
      forAll(RegExNonMatchingAddresses) {
        (pattern, host) ⇒
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier(Address(TestProtocol, TestSystem, host, TestPort)) should be(false)
      }
    }

  }

}
