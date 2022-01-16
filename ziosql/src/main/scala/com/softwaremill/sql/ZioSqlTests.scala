package com.softwaremill.sql

import zio.sql.postgresql.PostgresModule
import zio._

trait TableModel extends PostgresModule { self =>

    import ColumnSet._

}

object ZioSqlTests extends zio.App with DbSetup with TableModel {

    def run(args: List[String]): URIO[ZEnv ,ExitCode] = ???
    

}