package circuitbreaker

final class CircuitBreakerOpenException(message: String, cause: Throwable = None.orNull) extends Exception
