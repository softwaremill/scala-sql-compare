package com.softwaremill.sql

import com.softwaremill.sql.TrackType.TrackType
import io.getquill.{PostgresAsyncContext, SnakeCase}

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object QuillTests extends App with DbSetup {
  dbSetup()

  lazy val ctx = new PostgresAsyncContext[SnakeCase]("ctx")

  import ctx._

  // not necessary here, but just to demonstrate how to map to non-conventional db names
  val metroLines = quote {
    querySchema[MetroLine](
      "metro_line",
      _.id -> "id", // not all columns have to be specified
      _.stationCount -> "station_count"
    )
  }

  implicit val encodeTrackType = MappedEncoding[TrackType, Int](_.id)
  implicit val decodeTrackType = MappedEncoding[Int, TrackType] { id =>
    TrackType.values.find(_.id == id).getOrElse(throw new IllegalArgumentException(s"Unknown track type: $id"))
  }

  // note: we can use pure case classes, except for embedded values, which need to extend Embedded

  def insertWithGeneratedId(): Future[Unit] = {
    val q = quote {
      query[City].insert(lift(City(CityId(0), "New York", 19795791, 141300, None))).returning(_.id)
    }

    ctx.run(q).map { r =>
      println(s"Inserted, generated id: $r")
      println()
    }
  }

  def selectAll(): Future[Unit] = {
    val q = quote {
      query[City]
    }

    logResults("All cities", ctx.run(q))
  }

  def selectAllLines(): Future[Unit] = {
    val q = quote {
      query[MetroLine]
    }

    logResults("All lines", ctx.run(q))
  }

  def selectNamesOfBig(): Future[Unit] = {
    val bigLimit = 4000000

    val q = quote {
      query[City].filter(_.population > lift(bigLimit)).map(_.name)
    }

    logResults("All city names with population over 4M", ctx.run(q))
  }

  private def logResults[R](label: String, f: Future[Seq[R]]): Future[Unit] = {
    f.map { r =>
      println(label)
      r.foreach(println)
      println()
    }
  }

  val tests = for {
    _ <- insertWithGeneratedId()
    _ <- selectAll()
    _ <- selectAllLines()
    _ <- selectNamesOfBig()
  } yield ()

  try Await.result(tests, 1.minute)
  finally ctx.close()
}
