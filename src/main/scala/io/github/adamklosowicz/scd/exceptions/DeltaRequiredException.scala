package io.github.adamklosowicz.scd.exceptions

class DeltaRequiredException(path: String)
  extends RuntimeException(s"Path must be delta: $path")
