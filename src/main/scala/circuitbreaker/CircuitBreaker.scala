package circuitbreaker

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CircuitBreaker(val tripThreshold: Int = 5,
                     val coolDownPeriod: Long = 30000,
                     val failureCount: AtomicInteger = new AtomicInteger(0),
                     val lastTested: AtomicLong = new AtomicLong(0),
                     val currentTimeMillis: () => Long = System.currentTimeMillis) {
  def isClosed: Boolean = failureCount.get() < tripThreshold
  def isOpen: Boolean = !isClosed
  def isDueForTest: Boolean = isOpen && (lastTested.get() + coolDownPeriod) < currentTimeMillis()

  def apply[T](body: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    if (isClosed) return closed(body)
    if (isDueForTest) return test(body)
    open
  }

  private val testInProgress: AtomicBoolean = new AtomicBoolean(false)

  private def closed[T](body: => Future[T])(implicit ec: ExecutionContext): Future[T] =
    body andThen {
      case Success(_) =>
        failureCount.set(0)

      case Failure(_) =>
        failureCount.incrementAndGet()
        if(isOpen) lastTested.set(currentTimeMillis())
    }

  private def test[T](body: => Future[T])(implicit ec: ExecutionContext): Future[T] =
    if (testInProgress.compareAndSet(false, true)) {
      body andThen {
        case Success(_) =>
          failureCount.set(0)
        case Failure(_) =>
          lastTested.set(currentTimeMillis())
      }
    } else {
      open
    }

  private def open = throw new CircuitBreakerOpenException(s"Circuit breaker is open")
}
