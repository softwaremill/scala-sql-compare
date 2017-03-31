package com.softwaremill.sql

import com.softwaremill.sql.TrackType.TrackType
import org.flywaydb.core.Flyway
import slick.dbio.DBIOAction
import slick.jdbc.{JdbcProfile, PostgresProfile}
import slick.jdbc.JdbcBackend._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class Test {

}


case class CityId(id: Int) extends AnyVal
case class City(id: CityId, name: String, population: Int, area: Float, link: Option[String])

case class MetroSystemId(id: Int) extends AnyVal
case class MetroSystem(id: MetroSystemId, cityId: CityId, name: String, dailyRidership: Int)

case class MetroLineId(id: Int) extends AnyVal
case class MetroLine(id: MetroLineId, systemId: MetroSystemId, name: String, stationCount: Int, trackType: TrackType)

object TrackType extends Enumeration {
  type TrackType = Value
  val Rail = Value(1)
  val Monorail = Value(2)
  val Rubber = Value(3)
}

//

trait Schema {
  val jdbcProfile: JdbcProfile

  import jdbcProfile.api._

  implicit lazy val cityIdColumnType = MappedColumnType.base[CityId, Int](_.id, CityId)
  implicit lazy val metroSystemIdColumnType = MappedColumnType.base[MetroSystemId, Int](_.id, MetroSystemId)
  implicit lazy val metroLineIdColumnType = MappedColumnType.base[MetroLineId, Int](_.id, MetroLineId)
  implicit lazy val trackTypeColumnType = MappedColumnType.base[TrackType, Int](_.id,
    id => TrackType.values.find(_.id == id).getOrElse(throw new IllegalArgumentException(s"Unknown track type: $id")))

  class Cities(tag: Tag) extends Table[City](tag, "city") {
    def id = column[CityId]("id", O.PrimaryKey)
    def name = column[String]("name")
    def population = column[Int]("population")
    def area = column[Float]("area")
    def link = column[Option[String]]("link")
    def * = (id, name, population, area, link) <> (City.tupled, City.unapply)
  }
  lazy val cities = TableQuery[Cities]

  class MetroSystems(tag: Tag) extends Table[MetroSystem](tag, "metro_systems") {
    def id = column[MetroSystemId]("id", O.PrimaryKey)
    def cityId = column[CityId]("city_id")
    def name = column[String]("name")
    def dailyRidership = column[Int]("daily_ridership")
    def * = (id, cityId, name, dailyRidership) <> (MetroSystem.tupled, MetroSystem.unapply)
  }
  lazy val metroSystems = TableQuery[MetroSystems]

  class MetroLines(tag: Tag) extends Table[MetroLine](tag, "metro_line") {
    def id = column[MetroLineId]("id", O.PrimaryKey)
    def systemId = column[MetroSystemId]("system_id")
    def name = column[String]("name")
    def stationCount = column[Int]("station_count")
    def trackType = column[TrackType]("track_type")
    def * = (id, systemId, name, stationCount, trackType) <> (MetroLine.tupled, MetroLine.unapply)
  }
  lazy val metroLines = TableQuery[MetroLines]
}

object SlickTest extends App with Schema {
  val connectionString = "jdbc:postgresql:sql_compare"

  val db = Database.forURL(connectionString, driver = "org.postgresql.Driver")
  val jdbcProfile = PostgresProfile

  val flyway = new Flyway()
  flyway.setDataSource(connectionString, null, null)
  flyway.clean()
  flyway.migrate()

  try {
    import jdbcProfile.api._

    println(Await.result(db.run(metroLines.result), 1.minute))
  } finally db.close()
}