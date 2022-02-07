package com.softwaremill.sql

import com.dimafeng.testcontainers.SingleContainer
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import zio._

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
        ).configure { a =>
          a.withInitScript("init.sql")
          ()
        }
        c.start()
        c
      }
    } { container =>
      ZIO.effectBlocking(container.stop()).orDie
    }

}
