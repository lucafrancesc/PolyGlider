package com.polyglider.consumer

/** Classification of failures encountered while processing an order message.
  *
  * Classification rules:
  *  - JSON parsing/decoding errors (circe `ParsingFailure` / `DecodingFailure`) mean the
  *    payload itself is malformed. Retrying won't change the bytes on the wire, so these
  *    are Permanent.
  *  - Postgres integrity constraint violations (SQLState class "23") reflect a business-rule
  *    conflict baked into the input itself, not the infrastructure. Retrying produces the
  *    same conflict, so these are Permanent. (Duplicate `eventId`s no longer land here: the
  *    `processed_events` insert is `ON CONFLICT DO NOTHING`, so a dup is an idempotent ack,
  *    not a thrown exception.)
  *  - Postgres connection/availability errors (SQLState classes "08" connection exception,
  *    "53" insufficient resources, "57" operator intervention, "58" system error) and
  *    generic I/O or timeout failures are infrastructure blips that are likely to clear up
  *    on their own, so these are Transient.
  *  - Anything unrecognized defaults to Permanent. Failing closed into the DLQ for manual
  *    triage is safer than silently retry-looping forever on an error we don't understand.
  *  - `CircuitBreakerOpenException` (raised when the Postgres write path's circuit breaker is
  *    open) is Transient — the breaker tripped because of an infrastructure outage, not
  *    anything about this particular message.
  */
sealed trait ProcessingFailure {
  def cause: Throwable
}

object ProcessingFailure {
  final case class PermanentFailure(cause: Throwable) extends ProcessingFailure
  final case class TransientFailure(cause: Throwable) extends ProcessingFailure

  private val transientSqlStateClasses = Set("08", "53", "57", "58")
  private val permanentSqlStateClasses = Set("23")

  def classify(err: Throwable): ProcessingFailure = err match {
    case _: io.circe.ParsingFailure  => PermanentFailure(err)
    case _: io.circe.DecodingFailure => PermanentFailure(err)
    case _: com.polyglider.resilience.CircuitBreakerOpenException => TransientFailure(err)
    case sql: java.sql.SQLException if isInClass(sql.getSQLState, transientSqlStateClasses) =>
      TransientFailure(err)
    case sql: java.sql.SQLException if isInClass(sql.getSQLState, permanentSqlStateClasses) =>
      PermanentFailure(err)
    case _: java.sql.SQLTransientException        => TransientFailure(err)
    case _: java.io.IOException                    => TransientFailure(err)
    case _: java.util.concurrent.TimeoutException  => TransientFailure(err)
    case _                                          => PermanentFailure(err)
  }

  private def isInClass(sqlState: String, classes: Set[String]): Boolean =
    Option(sqlState).exists(s => classes.contains(s.take(2)))
}
