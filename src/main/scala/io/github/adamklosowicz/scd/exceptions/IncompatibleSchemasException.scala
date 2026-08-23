package io.github.adamklosowicz.scd.exceptions

class IncompatibleSchemasException(issues: Seq[String])
  extends RuntimeException("Schema comparison failed: " + issues.mkString("[", "; ", "]"))
