package akka.contrib.cluster.topology

import org.scalatest.prop.TableDrivenPropertyChecks._
import akka.ConfigurationException
import akka.testkit.AkkaSpec


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

}
