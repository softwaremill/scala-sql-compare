package com.softwaremill.sql

import org.flywaydb.core.Flyway

trait DbSetup {
  val connectionString = "jdbc:postgresql:sql_compare"

  def dbSetup(): Unit = {
    val flyway = new Flyway()
    flyway.setDataSource(connectionString, null, null)
    flyway.clean()
    flyway.migrate()
  }
}