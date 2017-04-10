package com.softwaremill.sql

import com.softwaremill.sql.TrackType.TrackType
import slick.dbio.Effect.Write
import slick.jdbc.{GetResult, JdbcBackend, JdbcProfile, PostgresProfile}
import slick.jdbc.JdbcBackend._
import slick.sql.SqlAction

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

trait Schema {
  val jdbcProfile: JdbcProfile

  import jdbcProfile.api._

  implicit lazy val cityIdColumnType = MappedColumnType.base[CityId, Int](_.id, CityId)
  implicit lazy val metroSystemIdColumnType = MappedColumnType.base[MetroSystemId, Int](_.id, MetroSystemId)
  implicit lazy val metroLineIdColumnType = MappedColumnType.base[MetroLineId, Int](_.id, MetroLineId)
  implicit lazy val trackTypeColumnType = MappedColumnType.base[TrackType, Int](_.id, TrackType.byIdOrThrow)

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

trait Queries extends Schema {
  val db: JdbcBackend#DatabaseDef
  val jdbcProfile: JdbcProfile

  import jdbcProfile.api._

  def insertWithGeneratedId(): Future[Unit] = {
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

  // many-to-one
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

  // we can extract and re-use common joins
  lazy val joinLinesToSystemsAndCities = metroLines
    .join(metroSystems).on(_.systemId === _.id)
    .join(cities).on(_._2.cityId === _.id)

  def selectMetroSystemsWithMostLines(): Future[Unit] = {
    case class MetroSystemWithLineCount(metroSystemName: String, cityName: String, lineCount: Int)

    val sqlQuery = joinLinesToSystemsAndCities
      .groupBy { case ((_, ms), c) => (ms.id, c.id, ms.name, c.name) }
      .map { case ((msId, cId, msName, cName), lines) => (msName, cName, lines.length) }
      .sortBy(_._3.desc)
      .result

    val query = sqlQuery.map(_.map(MetroSystemWithLineCount.tupled))

    runAndLogResults("Metro systems with most lines", sqlQuery, query)
  }

  // one-to-many
  def selectCitiesWithSystemsAndLines(): Future[Unit] = {
    case class CityWithSystems(id: CityId, name: String, population: Int, area: Float, link: Option[String], systems: Seq[MetroSystemWithLines])
    case class MetroSystemWithLines(id: MetroSystemId, cityId: CityId, name: String, dailyRidership: Int, lines: Seq[MetroLine])

    val sqlQuery = joinLinesToSystemsAndCities.result

    val query = sqlQuery.map(_
      .groupBy(_._2)
      .map { case (c, linesSystemsCities) =>
        val systems = linesSystemsCities.map(_._1).groupBy(_._2)
            .map { case (s, linesSystems) =>
            MetroSystemWithLines(s.id, s.cityId, s.name, s.dailyRidership, linesSystems.map(_._1))
          }
        CityWithSystems(c.id, c.name, c.population, c.area, c.link, systems.toSeq)
      })
      .map(_.toSeq)

    runAndLogResults("Cities with list of systems with list of lines", sqlQuery, query)
  }

  // http://stackoverflow.com/questions/31869919/dynamic-query-conditions-slick-3-0
  def selectLinesConstrainedDynamically(): Future[Unit] = {
    val minStations: Option[Int] = Some(10)
    val maxStations: Option[Int] = None
    val sortDesc: Boolean = true

    val query = metroLines
      .filter { line =>
        List(
          minStations.map(line.stationCount >= _),
          maxStations.map(line.stationCount <= _)
        ).flatten.reduceLeftOption(_ && _).getOrElse(true: LiteralColumn[Boolean])
      }
      .sortBy(l => if (sortDesc) l.stationCount.desc else l.stationCount.asc)
      .result

    runAndLogResults("Lines constrained dynamically", query, query)
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

  def transactions(): Future[Unit] = {
    def insertCity(name: String, population: Int, area: Float, link: Option[String]): DBIOAction[CityId, NoStream, Write] =
      (cities returning cities.map(_.id)) += City(CityId(0), name, population, area, link)

    def deleteCity(id: CityId): DBIOAction[Int, NoStream, Write] =
      cities.filter(_.id === id).delete

    val insertAndDelete = for {
      inserted <- insertCity("Invalid", 0, 0, None)
      deleted <- deleteCity(inserted)
    } yield deleted

    val result = db.run(insertAndDelete.transactionally)

    result.map { r =>
      println("Transactions")
      println(s"Deleted $r rows")
      println()
    }
  }

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
  dbSetup()

  val db = Database.forURL(connectionString, driver = "org.postgresql.Driver")
  val jdbcProfile = PostgresProfile

  try {
    val tests = for {
      _ <- insertWithGeneratedId()
      _ <- selectAll()
      _ <- selectNamesOfBig()
      _ <- selectMetroSystemsWithCityNames()
      _ <- selectMetroLinesSortedByStations()
      _ <- selectMetroSystemsWithMostLines()
      _ <- selectCitiesWithSystemsAndLines()
      _ <- selectLinesConstrainedDynamically()
      _ <- plainSql()
      //_ <- typeCheckedPlainSql()
      _ <- transactions()
    } yield ()

    Await.result(tests, 1.minute)
  } finally db.close()
}