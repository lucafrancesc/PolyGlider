package com.polyglider

import munit.CatsEffectSuite

class UuidUtilsSuite extends CatsEffectSuite {
  test("valid UUID returns true") {
    assert(UuidUtils.isValidUuid("11111111-1111-4111-8111-111111111111"))
  }

  test("invalid UUID returns false") {
    assert(!UuidUtils.isValidUuid("not-a-uuid"))
  }
}
