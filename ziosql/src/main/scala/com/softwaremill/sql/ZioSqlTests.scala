package com.softwaremill.sql

import zio.sql.postgresql.PostgresModule
import zio._

trait TableModel extends PostgresModule { 

    import ColumnSet._

    val city = (int("id") ++ string("name") ++ 
        int("population") ++ float("area") ++ string("link")).table("city")

    val cityId :*: cityName :*: population :*: area :*: link :*: _ = city.columns

    val metroSystem = (int("id") ++ int("city_id") ++ int("name") ++ int("daily_ridership")).table("metro_system")

    val metroSystemId :*: cityIdFk :*: metroSystemName :*: dailyRidership :*: _ = metroSystem.columns

    val metroLine = (int("id") ++ int("system_id") ++ string("name") ++ int("station_count") ++ int("track_type")).table("metro_line")

    val metroLineId :*: systemId :*: metroLineName :*: stationCount :*: trackType :*: _ = metroLine.columns
}

object ZioSqlTests extends zio.App with DbSetup with TableModel {

    def run(args: List[String]): URIO[ZEnv, ExitCode] = ???
    

}