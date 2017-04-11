package com.softwaremill.sql

import java.sql.ResultSet

import com.softwaremill.sql.TrackType.TrackType
import scalikejdbc._

object ScalikejdbcTests extends App with DbSetup {
  dbSetup()

  ConnectionPool.add('tests, "jdbc:postgresql:sql_compare", "postgres", "")
  def db: NamedDB = NamedDB('tests)

  GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(enabled = true, singleLineMode = true, logLevel = 'INFO)

  //

  implicit val cityIdTypeBinder: TypeBinder[CityId] = new TypeBinder[CityId] {
    def apply(rs: ResultSet, label: String): CityId = CityId(rs.getInt(label))
    def apply(rs: ResultSet, index: Int): CityId = CityId(rs.getInt(index))
  }

  implicit val metroSystemIdTypeBinder: TypeBinder[MetroSystemId] = new TypeBinder[MetroSystemId] {
    def apply(rs: ResultSet, label: String): MetroSystemId = MetroSystemId(rs.getInt(label))
    def apply(rs: ResultSet, index: Int): MetroSystemId = MetroSystemId(rs.getInt(index))
  }

  implicit val metroLineIdTypeBinder: TypeBinder[MetroLineId] = new TypeBinder[MetroLineId] {
    def apply(rs: ResultSet, label: String): MetroLineId = MetroLineId(rs.getInt(label))
    def apply(rs: ResultSet, index: Int): MetroLineId = MetroLineId(rs.getInt(index))
  }

  implicit val trackTypeTypeBinder: TypeBinder[TrackType] = new TypeBinder[TrackType] {
    def apply(rs: ResultSet, label: String): TrackType = TrackType.byIdOrThrow(rs.getInt(label))
    def apply(rs: ResultSet, index: Int): TrackType = TrackType.byIdOrThrow(rs.getInt(index))
  }

  class CitySQL(db: NamedDB) extends SQLSyntaxSupport[City] {
    override def connectionPoolName: Any = db.name
    override def tableName: String = "city"

    def apply(rs: WrappedResultSet, rn: ResultName[City]): City = autoConstruct[City](rs, rn)
  }

  class MetroSystemSQL(db: NamedDB) extends SQLSyntaxSupport[MetroSystem] {
    override def connectionPoolName: Any = db.name
    override def tableName: String = "metro_system"

    def apply(rs: WrappedResultSet, rn: ResultName[MetroSystem]): MetroSystem = autoConstruct[MetroSystem](rs, rn)
  }

  class MetroLineSQL(db: NamedDB) extends SQLSyntaxSupport[MetroLine] {
    override def connectionPoolName: Any = db.name
    override def tableName: String = "metro_line"

    def apply(rs: WrappedResultSet, rn: ResultName[MetroLine]): MetroLine = autoConstruct[MetroLine](rs, rn)
  }

  val citySQL = new CitySQL(db)
  val metroSystemSQL = new MetroSystemSQL(db)
  val metroLineSQL = new MetroLineSQL(db)

  //

  def insertCity(name: String, population: Int, area: Float, link: Option[String])(implicit session: DBSession): City = {
    // We can use sql interpolation:
    // val id = sql"insert into city(name, population, area, link) values ($name, $population, $area, $link)"
    //   .updateAndReturnGeneratedKey().apply()
    // Or:

    val c = citySQL.column
    val id = withSQL {
      insert.into(citySQL).namedValues(
        c.name -> name, // dynamic
        c.population -> population,
        c.area -> area,
        c.link -> link
      )
    }.updateAndReturnGeneratedKey().apply() // generated ids are assumed to be Long-s

    City(CityId(id.toInt), name, population, area, link)
  }

  def insertWithGeneratedId(): Unit = {
    val result = db.localTx { implicit session =>
      insertCity("New York", 19795791, 141300, None)
    }
    println(s"Inserted, generated id: ${result.id}")
    println()
  }

  def selectAll(): Unit = {
    val c = citySQL.syntax("c")
    val p = withSQL {
      select.from(citySQL as c)
    }.map(citySQL.apply(_, c.resultName)).list()

    // or:
    // val p = sql"select * from city".map(rs => City(CityId(rs.get[Int]("id")), rs.get[String]("name"),
    //   rs.get[Int]("population"), rs.get[Float]("area"), rs.get[Option[String]]("link"))).list

    runAndLogResults("All cities", p)
  }

  def selectAllLines(): Unit = {
    val ml = metroLineSQL.syntax("ml")
    val p = withSQL {
      select.from(metroLineSQL as ml)
    }.map(metroLineSQL.apply(_, ml.resultName)).list()

    runAndLogResults("All lines", p)
  }

  def selectNamesOfBig(): Unit = {
    val bigLimit = 4000000

    val c = citySQL.syntax("c")
    val p = withSQL {
      select.from(citySQL as c).where.gt(c.population, bigLimit)
    }.map(citySQL.apply(_, c.resultName)).list()

    runAndLogResults("All city names with population over 4M", p)
  }

  def selectMetroSystemsWithCityNames(): Unit = {
    case class MetroSystemWithCity(metroSystemName: String, cityName: String, dailyRidership: Int)

    val (ms, c) = (metroSystemSQL.syntax("ms"), citySQL.syntax("c"))
    val p = withSQL {
      select(ms.result.column("name"), c.result.column("name"), ms.result.dailyRidership)
        .from(metroSystemSQL as ms).leftJoin(citySQL as c).on(ms.cityId, c.id)
    }.map(rs => MetroSystemWithCity(rs.string(ms.resultName.name), rs.string(c.resultName.name), rs.int(ms.resultName.dailyRidership)))
      .list()

    runAndLogResults("Metro systems with city names", p)
  }

  def selectMetroLinesSortedByStations(): Unit = {
    case class MetroLineWithSystemCityNames(metroLineName: String, metroSystemName: String, cityName: String, stationCount: Int)

    val (ml, ms, c) = (metroLineSQL.syntax("ml"), metroSystemSQL.syntax("ms"), citySQL.syntax("c"))
    val p = withSQL {
      select(ml.result.column("name"), ms.result.column("name"), c.result.column("name"), ml.result.stationCount)
        .from(metroLineSQL as ml)
        .join(metroSystemSQL as ms).on(ml.systemId, ms.id)
        .join(citySQL as c).on(ms.cityId, c.id)
        .orderBy(ml.stationCount).desc
    }
      .map(rs => MetroLineWithSystemCityNames(rs.string(ml.resultName.name), rs.string(ms.resultName.name),
        rs.string(c.resultName.name), rs.int(ml.resultName.stationCount)))
      .list()

    runAndLogResults("Metro lines sorted by station count", p)
  }

  def selectMetroSystemsWithMostLines(): Unit = {
    case class MetroSystemWithLineCount(metroSystemName: String, cityName: String, lineCount: Int)

    val (ml, ms, c) = (metroLineSQL.syntax("ml"), metroSystemSQL.syntax("ms"), citySQL.syntax("c"))
    val p = withSQL {
      select(ms.result.column("name"), c.result.column("name"), c.result.column("name"), sqls"count(ml.id) as line_count")
        .from(metroLineSQL as ml)
        .join(metroSystemSQL as ms).on(ml.systemId, ms.id)
        .join(citySQL as c).on(ms.cityId, c.id)
        .groupBy(ms.id, c.id)
        .orderBy(sqls"line_count").desc
    }
      .map(rs => MetroSystemWithLineCount(rs.string(ms.resultName.name), rs.string(c.resultName.name),
        rs.int("line_count")))
      .list()

    runAndLogResults("Metro systems with most lines", p)
  }

  def selectCitiesWithSystemsAndLines(): Unit = {
    case class SystemWithLines(cityId: CityId, cityName: String, population: Int, area: Float, link: Option[String],
      systemId: MetroSystemId, systemName: String, dailyRidership: Int, lines: Seq[MetroLine])

    case class CityWithSystems(id: CityId, name: String, population: Int, area: Float, link: Option[String], systems: Seq[MetroSystemWithLines])
    case class MetroSystemWithLines(id: MetroSystemId, name: String, dailyRidership: Int, lines: Seq[MetroLine])

    val (ml, ms, c) = (metroLineSQL.syntax("ml"), metroSystemSQL.syntax("ms"), citySQL.syntax("c"))
    val p: SQLToList[SystemWithLines, HasExtractor] = withSQL {
      select
        .from(metroLineSQL as ml)
        .join(metroSystemSQL as ms).on(ml.systemId, ms.id)
        .join(citySQL as c).on(ms.cityId, c.id)
    }
      .one(rs => SystemWithLines(CityId(rs.int(c.resultName.id)), rs.string(c.resultName.name), rs.int(c.resultName.population),
        rs.float(c.resultName.area), rs.stringOpt(c.resultName.link), MetroSystemId(rs.int(ms.resultName.id)), rs.string(ms.resultName.name),
        rs.int(ms.resultName.dailyRidership), Nil))
      .toMany(rs => Some(metroLineSQL(rs, ml.resultName)))
      .map { (cws, mls) => cws.copy(lines = mls.toList) }
      .list()

    println("Cities with list of systems with list of lines")
    db.readOnly { implicit session =>
      p.apply()
        .groupBy(swl => CityWithSystems(swl.cityId, swl.cityName, swl.population, swl.area, swl.link, Nil))
        .map { case (cws, swls) =>
          cws.copy(systems = swls.map(swl => MetroSystemWithLines(swl.systemId, swl.systemName, swl.dailyRidership, swl.lines)))
        }
        .foreach(println)
    }
    println()
  }

  def selectLinesConstrainedDynamically(): Unit = {
    val minStations: Option[Int] = Some(10)
    val maxStations: Option[Int] = None
    val sortDesc: Boolean = true

    val ml = metroLineSQL.syntax("ml")
    val p = withSQL {
      // can't assign to a val, as the [A] is "lost" and causes compilation errors
      select.from(metroLineSQL as ml)
        .where(sqls.toAndConditionOpt(
          minStations.map(ms => sqls.ge(ml.stationCount, ms)),
          maxStations.map(ms => sqls.le(ml.stationCount, ms))
        ))
        .orderBy(ml.stationCount)
        .append(if (sortDesc) sqls"desc" else sqls"asc")
    }.map(metroLineSQL.apply(_, ml.resultName)).list()

    runAndLogResults("Lines constrained dynamically", p)
  }

  def transactions(): Unit = {
    def deleteCity(id: CityId)(implicit session: DBSession): Int = {
      withSQL {
        val c = citySQL.syntax("c")
        delete.from(citySQL as c).where.eq(c.id, id.id)
      }.update().apply()
    }

    println("Transactions")
    val deletedCount = db.localTx { implicit session =>
      val inserted = insertCity("Invalid", 0, 0, None)
      deleteCity(inserted.id)
    }
    println(s"Deleted $deletedCount rows")
    println()
  }

  def runAndLogResults[R](label: String, program: SQLToList[R, HasExtractor]): Unit = {
    println(label)
    db.readOnly { implicit session =>
      program.apply().foreach(println)
    }
    println()
  }

  insertWithGeneratedId()
  selectAll()
  selectAllLines()
  selectNamesOfBig()
  selectMetroSystemsWithCityNames()
  selectMetroLinesSortedByStations()
  selectMetroSystemsWithMostLines()
  selectCitiesWithSystemsAndLines()
  selectLinesConstrainedDynamically()
  transactions()
}