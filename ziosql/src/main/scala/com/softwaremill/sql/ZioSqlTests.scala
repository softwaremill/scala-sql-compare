package com.softwaremill.sql

import zio.sql.postgresql.PostgresModule
import zio._
import zio.sql.ConnectionPoolConfig
import zio.sql.ConnectionPool
import java.util.Properties
import org.flywaydb.core.Flyway
import java.io.IOException
import java.util.concurrent.TimeUnit

trait TableModel extends PostgresModule { 

    import ColumnSet._

    val city = (int("id") ++ string("name") ++ 
        int("population") ++ float("area") ++ string("link")).table("city")

    val (cityId, cityName, population, area, link) = city.columns

    val metroSystem = (int("id") ++ int("city_id") ++ int("name") ++ int("daily_ridership")).table("metro_system")

    val (metroSystemId, cityIdFk, metroSystemName, dailyRidership) = metroSystem.columns

    val metroLine = (int("id") ++ int("system_id") ++ string("name") ++ int("station_count") ++ int("track_type")).table("metro_line")

    val (metroLineId, systemId, metroLineName, stationCount, trackType) = metroLine.columns
}

object ZioSqlTests extends ZIOAppDefault with TableModel {

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
            i       <- execute(insertQuery)
            cities  <- executeCities(citiesBiggerThan(0))
            _       <- Console.printLine(s"Cities: \n ${cities.mkString("\n")}")
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
                    City(CityId(id), name, pop, area, Option(link))
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
            .join(metroSystem).on(metroSystemId == systemId)
            .join(city).on(cityIdFk == cityId))
        .groupBy(metroSystemName, cityName)
        .orderBy(Desc(lineCount))


    // DYNAMIC
    val base = select(metroLineId ++ systemId ++ metroLineName ++ stationCount ++ trackType).from(metroLine)

    val minStations: Option[Int] = Some(10)
    val maxStations: Option[Int] = None
    val sortDesc: Boolean = true

    val minStationsQuery = minStations.map(m => stationCount >= m)
    val maxStationsQuery = maxStations.map(m => stationCount <= m)

    val ord = 
        if (sortDesc) 
            stationCount.desc 
        else 
            stationCount.asc

    val whereExpr = 
        List(minStationsQuery, maxStationsQuery)
            .flatten
            .reduceLeftOption[Expr[_, metroLine.TableType, Boolean]](_ && _)
            .get

    val finalQuery = base.where(whereExpr).orderBy(ord)

    // INSERT
    val insertQuery = insertInto(city)(cityId ++ cityName ++ population ++ area ++ link)
        .values((4, "London", 8982000, 1583F, "https://tfl.gov.uk/modes/tube/"))


    // TRANSACTIONS
    val transaction = for {
        insert <- ZTransaction(insertQuery)
        cities <- ZTransaction(citiesBiggerThan(8000000))
    } yield (cities)

    val cities: ZIO[SqlDriver, Exception, Chunk[City]] = execute(transaction)
        .use(stream => stream.runCollect)
        
    //PLAIN SQL

    val complexQuerySql = renderRead(complexQuery)
    val insertSql = renderInsert(insertQuery)
}