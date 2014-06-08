package akka.contrib.cluster.topology

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.prop.TableDrivenPropertyChecks._

/**
 *
 *
 * @author Lukasz Olczak
 */
class AddressClassifierSpec extends WordSpec with Matchers {

  val invalidPatterns = Table(
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
    "[a-z]*dev.some.org"
  )

  val validNetMaskPatterns = Table(
    "net:192.168.0.0/16",
    "net:192.168.0.0/16",
    "net:192.168.0.0/8",
    "net:192.168.0.0/24",
    "net:1.0.0.0/1",
    "net:192.168.0.0/1")

  val validRegExPatterns = Table(
    "reg-ex:[a-z]*dev\\.domain\\.org",
    "reg-ex:[a-z]*\\.1\\.datacenter1\\.domain\\.org",
    "reg-ex:[a-z]*dev\\.some\\.org")

  "An AddressClassifier" must {

    "throw exception upon malformed address pattern" in {
      forAll(invalidPatterns) {
        pattern =>
          intercept[IllegalArgumentException] {
            AddressClassifier.fromString(pattern)
          }
      }
    }

    "support parsing netmask pattern" in {
      forAll(validNetMaskPatterns) {
        pattern =>
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier shouldNot be(null)
      }
    }

    "support parsing reg ex pattern" in {
      forAll(validRegExPatterns) {
        pattern =>
          val addressClassifier = AddressClassifier.fromString(pattern)
          addressClassifier shouldNot be(null)
      }
    }

  }

}
