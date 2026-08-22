package io.github.adamklosowicz.scd.exceptions

class InvalidScdTypeException(scdType: String)
  extends RuntimeException(s"Invalid SCD type: $scdType")
