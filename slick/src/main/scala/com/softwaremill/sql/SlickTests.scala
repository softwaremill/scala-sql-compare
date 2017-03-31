package com.softwaremill.sql

import com.softwaremill.sql.TrackType.TrackType
import org.flywaydb.core.Flyway
import slick.basic.StaticDatabaseConfig
import slick.jdbc.{GetResult, JdbcBackend, JdbcProfile, PostgresProfile}
import slick.jdbc.JdbcBackend._
import slick.sql.SqlAction

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

//

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
    def id = column[CityId]("id", O.PrimaryKey, O.AutoInc)
    def name = column[String]("name")
    def population = column[Int]("population")
    def area = column[Float]("area")
    def link = column[Option[String]]("link")
    def * = (id, name, population, area, link) <> (City.tupled, City.unapply)
  }
  lazy val cities = TableQuery[Cities]

  class MetroSystems(tag: Tag) extends Table[MetroSystem](tag, "metro_system") {
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

trait DbSetup {
  def connectionString: String

  def dbSetup(): Unit = {
    val flyway = new Flyway()
    flyway.setDataSource(connectionString, null, null)
    flyway.clean()
    flyway.migrate()
  }
}

trait Queries extends Schema {
  val db: JdbcBackend#DatabaseDef
  val jdbcProfile: JdbcProfile

  import jdbcProfile.api._

  def insertWithId(): Future[Unit] = {
    val insertQuery = (cities returning cities.map(_.id)) += City(CityId(0), "New York", 19795791, 141300, None)
    db.run(insertQuery).map { r =>
      println(s"Inserted, generated id: $r")
      println()
    }
  }

  def selectAll(): Future[Unit] = {
    val query = cities.result
    runAndLogResults("All cities", query, query)
  }

  def selectNamesOfBig(): Future[Unit] = {
    val query = cities.filter(_.population > 4000000).map(c => (c.name, c.population)).result
    runAndLogResults("All city names with population over 4M", query, query)
  }

  def selectMetroSystemsWithCityNames(): Future[Unit] = {
    case class MetroSystemWithCity(metroSystemName: String, cityName: String, dailyRidership: Int)

    val sqlQuery = (for {
      (ms, c) <- metroSystems join cities on (_.cityId === _.id)
    } yield (ms.name, c.name, ms.dailyRidership)).result

    val query = sqlQuery.map(_.map(MetroSystemWithCity.tupled))
    
    runAndLogResults("Metro systems with city names", sqlQuery, query)
  }

  def selectMetroLinesSortedByStations(): Future[Unit] = {
    case class MetroLineWithSystemCityNames(metroLineName: String, metroSystemName: String, cityName: String, stationCount: Int)

    // could use lifted case classes to avoid the _2, _4: http://slick.lightbend.com/doc/3.0.0/userdefined.html#monomorphic-case-classes
    val sqlQuery = (for {
      ((ml, ms), c) <- metroLines
        .join(metroSystems).on(_.systemId === _.id)
        .join(cities).on(_._2.cityId === _.id)
    } yield (ml.name, ms.name, c.name, ml.stationCount)).sortBy(_._4.desc).result

    /*
    Alternatively:
    val sqlQuery = (for {
      ml <- metroLines
      ms <- metroSystems if ms.id === ml.systemId
      c <- cities if c.id === ms.cityId
    } yield (ml.name, ms.name, c.name, ml.stationCount)).sortBy(_._4.desc).result
    */

    val query = sqlQuery.map(_.map(MetroLineWithSystemCityNames.tupled))

    runAndLogResults("Metro lines sorted by station count", sqlQuery, query)
  }

  def selectMetroSystemsWithMostLines(): Future[Unit] = {
    case class MetrySystemWithLineCount(metroSystemName: String, cityName: String, lineCount: Int)

    // we can extract common joins
    val joinLinesToSystemsAndCities = metroLines
      .join(metroSystems).on(_.systemId === _.id)
      .join(cities).on(_._2.cityId === _.id)

    val sqlQuery = joinLinesToSystemsAndCities
      .groupBy { case ((_, ms), c) => (ms.id, c.id, ms.name, c.name) }
      .map { case ((msId, cId, msName, cName), lines) => (msName, cName, lines.length) }
      .sortBy(_._3.desc)
      .result

    val query = sqlQuery.map(_.map(MetrySystemWithLineCount.tupled))

    runAndLogResults("Metro systems with most lines", sqlQuery, query)
  }

  def plainSql(): Future[Unit] = {
    case class MetroSystemWithCity(metroSystemName: String, cityName: String, dailyRidership: Int)

    implicit val getMetroSystemWithCityResult = GetResult(r => MetroSystemWithCity(r.nextString, r.nextString, r.nextInt))

    val query = sql"""SELECT ms.name, c.name, ms.daily_ridership
                             FROM metro_system as ms
                             JOIN city AS c ON ms.city_id = c.id
                             ORDER BY ms.daily_ridership DESC""".as[MetroSystemWithCity]

    runAndLogResults("Plain sql", query)
  }

  // The path here is problematic. Works from sbt, doesn't work from ItelliJ.
  /*
  @StaticDatabaseConfig("file:slick/src/main/resources/application.conf#tsql")
  def typeCheckedPlainSql(): Future[Unit] = {
    case class MetroSystemWithCity(metroSystemName: String, cityName: String, dailyRidership: Int)

    implicit val getMetroSystemWithCityResult = GetResult(r => MetroSystemWithCity(r.nextString, r.nextString, r.nextInt))

    val query = tsql"""SELECT ms.name, c.name, ms.daily_ridership
                              FROM metro_system as ms
                              JOIN city AS c ON ms.city_id = c.id
                              ORDER BY ms.daily_ridership DESC""".map(_.map(MetroSystemWithCity.tupled))

    runAndLogResults("Type checked plain sql", query)
  }
  */

  /**
    * @param sqlQuery The "raw" sql query (without result mapping) is needed to log the generated SQL.
    */
  private def runAndLogResults[R](label: String, sqlQuery: SqlAction[Seq[R], NoStream, _], query: DBIOAction[Seq[R], NoStream, _]): Future[Unit] = {
    db.run(query).map { r =>
      println(label)
      r.foreach(println)
      println("Generated query:")
      println(sqlQuery.statements.mkString("\n"))
      println()
    }
  }

  private def runAndLogResults[R](label: String, query: DBIOAction[Seq[R], NoStream, _]): Future[Unit] = {
    db.run(query).map { r =>
      println(label)
      r.foreach(println)
      println()
    }
  }
}

object SlickTests extends App with Schema with DbSetup with Queries {
  val connectionString = "jdbc:postgresql:sql_compare"

  val db = Database.forURL(connectionString, driver = "org.postgresql.Driver")
  val jdbcProfile = PostgresProfile

  dbSetup()

  try {
    val tests = for {
      _ <- insertWithId()
      _ <- selectAll()
      _ <- selectNamesOfBig()
      _ <- selectMetroSystemsWithCityNames()
      _ <- selectMetroLinesSortedByStations()
      _ <- selectMetroSystemsWithMostLines()
      _ <- plainSql()
      //_ <- typeCheckedPlainSql()
    } yield ()

    Await.result(tests, 1.minute)
  } finally db.close()
}