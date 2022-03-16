package com.softwaremill.sql

import com.dimafeng.testcontainers.SingleContainer
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import zio._
import org.flywaydb.core.Flyway

object TestContainer {

  def container[C <: SingleContainer[_]: Tag](c: C): ZLayer[Any, Throwable, C] =
    ZManaged.make {
      ZIO.effectBlocking {
        c.start()
        c
      }
    }(container => ZIO.effectBlocking(container.stop()).orDie).toLayer

  def postgres(imageName: String = "postgres:alpine"): ZManaged[Any, Throwable, PostgreSQLContainer] =
    ZManaged.make {
      ZIO.effectBlocking {
        val c = new PostgreSQLContainer(
          dockerImageNameOverride = Option(imageName).map(DockerImageName.parse)
        )
        c.start()
        val flyway = new Flyway()
        flyway.setDataSource(c.container.getJdbcUrl(), c.container.getUsername(), c.container.getPassword())
        flyway.migrate()
        c
      }
    } { container =>
      ZIO.effectBlocking(container.stop()).orDie
    }
}