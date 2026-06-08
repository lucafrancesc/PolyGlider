package com.polyglider

import java.util.UUID

object UuidUtils {
  def isValidUuid(s: String): Boolean =
    try {
      UUID.fromString(s)
      true
    } catch {
      case _: Throwable => false
    }
}
