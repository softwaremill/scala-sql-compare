package com.softwaremill.sql

import zio.sql.postgresql.PostgresModule
import zio._
import zio.stream._
import zio.sql.ConnectionPoolConfig
import zio.sql.ConnectionPool
import java.util.Properties
import org.flywaydb.core.Flyway
import java.io.IOException
import java.util.concurrent.TimeUnit

trait TableModel extends PostgresModule { 

    import ColumnSet._
    import ColumnSetAspect._

    // Mapping

    val city = (int("id") ++ string("name") ++ 
        int("population") ++ float("area") ++ (string("link") @@ nullable)).table("city")

    val (cityId, cityName, population, area, link) = city.columns

    val metroSystem = (int("id") ++ int("city_id") ++ string("name") ++ int("daily_ridership")).table("metro_system")

    val (metroSystemId, cityIdFk, metroSystemName, dailyRidership) = metroSystem.columns

    val metroLine = (int("id") ++ int("system_id") ++ string("name") ++ int("station_count") ++ int("track_type")).table("metro_line")

    val (metroLineId, systemId, metroLineName, stationCount, trackType) = metroLine.columns
}

object ZioSqlTests extends ZIOAppDefault with TableModel {

    // DB Connection

    val poolConfigLayer = TestContainer
        .postgres()
        .map(a =>
            ConnectionPoolConfig(
                url = a.jdbcUrl,
                properties = connProperties(a.username, a.password),
            )
        )
        .toLayer

    private def connProperties(user: String, password: String): Properties = {
      val props = new Properties
      props.setProperty("user", user)
      props.setProperty("password", password)
      props
    }

    final lazy val driverLayer = ZLayer.make[SqlDriver](
        poolConfigLayer,
        ConnectionPool.live,
        Clock.live,
        SqlDriver.live
    )

    import AggregationDef._
    import Ordering._

    def run = 
        (for {
            allCities                  <- selectAllCities
            _                          <- ZIO.logInfo(s"All cities: \n ${allCities.mkString("\n")}")
            metroSystemWithCityName    <- selectMetroSystemsWithCityNames
            _                          <- ZIO.logInfo(s"Metro systems with city names: \n ${metroSystemWithCityName.mkString("\n")}, size ${metroSystemWithCityName.count(_ => true)}")
            metroLinesSortedByStations <- selectMetroLinesSortedByStations
            _                          <- ZIO.logInfo(s"Metro lines sorted by station count: \n ${metroLinesSortedByStations.mkString("\n")}")
            metroSystemWithMostLines   <- selectMetroSystemsWithMostLines
            _                          <- ZIO.logInfo(s"Metro systems sorted by most stations: \n ${metroSystemWithMostLines.mkString("\n")}")
            citiesWithSystemsAndLines  <- selectCitiesWithSystemsAndLines
            _                          <- ZIO.logInfo(s"Cities with metro systems and metro lines: \n ${citiesWithSystemsAndLines.mkString("\n")}")
            _                          <- execute(insertCity(CityId(4)))
            bigCities                  <- executeCities(citiesBiggerThan(0))
            _                          <- ZIO.logInfo(s"Big cities: \n ${bigCities.mkString("\n")}")
            rows                       <- deletedRows.useNow
            _                          <- ZIO.logInfo(s"Rows deleted: ${rows}")
            _                          <- ZIO.logInfo(s"Rendered complex query: \n ${complexQuerySql} \n")
            _                          <- ZIO.logInfo(s"Rendered insert query : \n ${insertSql} \n")
        } yield ()).provideCustomLayer(driverLayer)
        

    // SIMPLE

    /**
      * select id, name, population, area, link from city where population > $limit
      */
    def citiesBiggerThan(people: Int) = 
        select(cityId ++ cityName ++ population ++ area ++ link)
            .from(city)
            .where(population > people)
            .to {
                case (id, name, pop, area, link) =>
                    City(CityId(id), name, pop, area, link)
            }   

    def executeCities(cityQuery: Read[City]) = 
        execute(cityQuery)
            .runCollect
            .map(_.toList)

    val result = executeCities(citiesBiggerThan(4000000))

    // COMPLEX            

    /**
        SELECT ms.name, c.name, COUNT(ml.id) as line_count
        FROM metro_line as ml
        JOIN metro_system as ms on ml.system_id = ms.id
        JOIN city AS c ON ms.city_id = c.id
        GROUP BY ms.name, c.name
        ORDER BY line_count DESC
    */
    val lineCount = (Count(metroLineId) as "line_count")              

    val complexQuery = select(metroLineName ++ cityName ++ lineCount)
        .from(metroLine
            .join(metroSystem).on(metroSystemId === systemId)
            .join(city).on(cityIdFk === cityId))
        .groupBy(metroLineName, cityName)
        .orderBy(Desc(lineCount))


    // DYNAMIC

    val base = select(metroLineId ++ systemId ++ metroLineName ++ stationCount ++ trackType).from(metroLine)

    val minStations: Option[Int] = Some(10)
    val maxStations: Option[Int] = None
    val sortDesc: Boolean = true

    val minStationsQuery = minStations.map(m => stationCount >= m).getOrElse(Expr.literal(true))
    val maxStationsQuery = maxStations.map(m => trackType <= m).getOrElse(Expr.literal(true))

    val ord = 
        if (sortDesc) 
            stationCount.desc 
        else 
            stationCount.asc

    val whereExpr =
        minStationsQuery && maxStationsQuery

    val finalQuery = base.where(whereExpr).orderBy(ord)

    // INSERT

    def insertCity(id: CityId) = insertInto(city)(cityId ++ cityName ++ population ++ area ++ link)
        .values((id.id, "London", 8982000, 1583F, Option("https://tfl.gov.uk/modes/tube/")))

    // DELETE

    def deleteCity(id: CityId) = 
        deleteFrom(city)
            .where(cityId === id.id)

    // TRANSACTIONS

    val id = CityId(5)

    val transaction: ZTransaction[Any, Exception, Int] = for {
        _    <- ZTransaction(insertCity(id))
        rows <- ZTransaction(deleteCity(id))
    } yield (rows)

    val deletedRows: ZManaged[SqlDriver, Exception, Int] = execute(transaction)
        
    // render to PLAIN SQL

    val complexQuerySql = renderRead(complexQuery)
    val insertSql = renderInsert(insertCity(id))

    // OTHER EXAMPLES

    val selectAllCities: ZIO[SqlDriver, Throwable, Chunk[City]] = {
        val all = select(cityId ++ cityName ++ population ++ area ++ link)
            .from(city)
            .to{
                case (id, name, population, area, link) => City(CityId(id), name , population, area, link)
            }
        execute(all)
            .runCollect
    }

    final case class MetroSystemWithCity(metroSystemName: String, cityName: String, dailyRidership: Int)

    val selectMetroSystemsWithCityNames: ZIO[SqlDriver, Throwable, Chunk[MetroSystemWithCity]] = {
        val query = select(metroSystemName ++ cityName ++ dailyRidership)
            .from(metroSystem
                .join(city)
                .on(cityId === cityIdFk)
                )
            .to(MetroSystemWithCity.tupled)

        

        execute(query)
            .runCollect
    }

    case class MetroLineWithSystemCityNames(metroLineName: String, metroSystemName: String, cityName: String, stationCount: Int)

    val selectMetroLinesSortedByStations: ZIO[SqlDriver, Throwable, Chunk[MetroLineWithSystemCityNames]] = {

        val query = select(metroLineName ++ metroSystemName ++ cityName ++ stationCount)
            .from(metroLine
                .join(metroSystem)
                .on(systemId === metroSystemId)
                .join(city)
                .on(cityIdFk === cityId))
            .orderBy(Desc(stationCount))
            .to(MetroLineWithSystemCityNames.tupled)
                
        execute(query)
            .runCollect
    }

    final case class MetroSystemWithLineCount(metroSystemName: String, cityName: String, lineCount: Long)        
    
    val selectMetroSystemsWithMostLines: ZIO[SqlDriver, Throwable, Chunk[MetroSystemWithLineCount]]  = {
        val query = select(metroLineName ++ cityName ++ Count(metroLineId))
            .from(metroLine
                .join(metroSystem).on(metroSystemId === systemId)
                .join(city).on(cityIdFk === cityId))
            .groupBy(metroLineName, cityName)
            .orderBy(Desc(Count(metroLineId)))
            .to(MetroSystemWithLineCount.tupled)

        execute(query)
            .runCollect
    }

    final case class CityWithSystems(id: CityId, name: String, population: Int, area: Float, link: Option[String], systems: Seq[MetroSystemWithLines])
    final case class MetroSystemWithLines(id: MetroSystemId, name: String, dailyRidership: Int, lines: Seq[MetroLine])

    val selectCitiesWithSystemsAndLines:  ZIO[SqlDriver, Throwable, List[CityWithSystems]] = {

        val query = select(cityId ++ cityName ++ population ++ area ++ link ++ 
                            metroSystemId ++ cityIdFk ++ metroSystemName ++ dailyRidership ++ 
                            metroLineId ++ systemId ++ metroLineName ++ stationCount ++ trackType)
                    .from(metroLine
                        .join(metroSystem).on(metroSystemId === systemId)
                        .join(city).on(cityIdFk === cityId))
                    .to {
                        case (cityId, cityName, population, area, link, 
                            metroSystemId, cityIdFk, metroSystemName, dailyRidership, 
                            metroLineId, systemId, metroLineName, stationCount, trackType) =>
                                (City(CityId(cityId), cityName, population, area, link), 
                                 MetroSystem(MetroSystemId(metroSystemId), CityId(cityIdFk), metroLineName, dailyRidership), 
                                 MetroLine(MetroLineId(metroLineId), MetroSystemId(systemId), metroLineName, stationCount, TrackType.byIdOrThrow(trackType)))
                    }

        execute(query)
            .runCollect
            .map(l => l.groupBy(_._1).map {
                case (c, citiesSystemsLines) => 
                    val systems = citiesSystemsLines.groupBy(_._2)
                        .map { case (s, systemsLines) =>
                            MetroSystemWithLines(s.id, s.name, s.dailyRidership, systemsLines.map(_._3))
                        }
                    CityWithSystems(c.id, c.name, c.population, c.area, c.link, systems.toSeq)
            })
            .map(_.toList)
    }
}