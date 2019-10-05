package circuitbreaker

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.scalatest._

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class CircuitBreakerSpec extends AsyncWordSpec with Matchers with OptionValues with Inside with Inspectors {
  "A CircuitBreaker" when {
    "closed" should {
      val cb = new CircuitBreaker(tripThreshold = 2, failureCount = new AtomicInteger(1))

      "be closed" in assert(cb.isClosed)

      "not be open" in assert(!cb.isOpen)

      "invoke block and reset failureCount on success" in {
        cb { Future.successful("great success") } map { result =>
          assert(result === "great success")
          assert(cb.failureCount.get() === 0)
        }
      }
    }

    "tripThreshold crossed" should {
      val cb = new CircuitBreaker(
        tripThreshold = 2,
        failureCount = new AtomicInteger(1),
        currentTimeMillis = () => 123
      )

      "invoke block, increase failureCount and trip breaker on failure" in {
        cb { Future.failed(new Exception("test")) } transform {
          case Failure(_) => Success {
            assert(cb.failureCount.get() === 2)
            assert(cb.isOpen)
            assert(cb.lastTested.get() === 123)
          }
          case _ => Success(fail)
        }
      }
    }

    "open, during cool-down period" should {
      val cb = new CircuitBreaker(
        tripThreshold = 5,
        coolDownPeriod = 10,
        failureCount = new AtomicInteger(5),
        lastTested = new AtomicLong(100),
        currentTimeMillis = () => 110
      )

      "not be closed" in assert(!cb.isClosed)

      "be open" in assert(cb.isOpen)

      "not be due for test" in assert(!cb.isDueForTest)

      "throw instead of invoking block" in {
        var invoked = false
        assertThrows[CircuitBreakerOpenException](cb { Future.successful{ invoked = true } })
        assert(!invoked)
      }
    }

    "open, testing" should {
      "be due for test after cool-down period" in {
        val cb = new CircuitBreaker(
          tripThreshold = 5,
          coolDownPeriod = 10,
          failureCount = new AtomicInteger(5),
          lastTested = new AtomicLong(100),
          currentTimeMillis = () => 111
        )

        assert(cb.isDueForTest)
      }

      "invoke block and reset breaker on success" in {
        val cb = new CircuitBreaker(
          tripThreshold = 5,
          coolDownPeriod = 10,
          failureCount = new AtomicInteger(5),
          lastTested = new AtomicLong(100),
          currentTimeMillis = () => 130
        )

        cb { Future.successful("great success") } map { result =>
          assert(result === "great success")
          assert(cb.failureCount.get() === 0)
          assert(cb.isClosed)
        }
      }

      "invoke block and reset lastTested on failure" in {
        val cb = new CircuitBreaker(
          tripThreshold = 5,
          coolDownPeriod = 10,
          failureCount = new AtomicInteger(5),
          lastTested = new AtomicLong(100),
          currentTimeMillis = () => 130
        )

        cb { Future.failed(new Exception("test")) } transform {
          case Failure(_) => Success {
            assert(cb.isOpen)
            assert(cb.lastTested.get() === 130)
            assert(!cb.isDueForTest)
          }
          case _ => Success(fail)
        }
      }

      "allow only one concurrent invocation of block" in {
        val cb = new CircuitBreaker(
          tripThreshold = 5,
          coolDownPeriod = 10,
          failureCount = new AtomicInteger(5),
          lastTested = new AtomicLong(100),
          currentTimeMillis = () => 130
        )
        val count = new AtomicInteger(0)

        def f(x: Int): Future[Int] = Try(
          cb { Future.successful(count.incrementAndGet()) }
        ) getOrElse Future.successful(x)

        Future.sequence(
          Seq.tabulate(10)(f)
        ) transform {
          case Success(_) => Success {
            assert(count.get() === 1)
          }
          case _ => Success(fail)
        }
      }
    }
  }
}
