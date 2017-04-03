package com.softwaremill.sql

import doobie.imports._
import cats._, cats.data._, cats.implicits._
import fs2.interop.cats._

object DoobieTests extends App with DbSetup {
  dbSetup()

  val xa = DriverManagerTransactor[IOLite](
    "org.postgresql.Driver", "jdbc:postgresql:sql_compare", null, null)

  def insertWithGeneratedId(): Unit = {
    def insertCity(name: String, population: Int, area: Float, link: Option[String]): ConnectionIO[City] = {
      sql"insert into city(name, population, area, link) values ($name, $population, $area, $link)"
        .update.withUniqueGeneratedKeys[CityId]("id")
        .map(id => City(id, name, population, area, link))
    }

    val result = insertCity("New York", 19795791, 141300, None).transact(xa).unsafePerformIO
    println(s"Inserted, generated id: ${result.id}")
    println()
  }

  def selectAll(): Unit = {
    val program = sql"select id, name, population, area, link from city"
      .query[City]
      .list

    runAndLogResults("All cities", program)
  }

  def selectNamesOfBig(): Unit = {
    val bigLimit = 4000000

    val program = sql"select id, name, population, area, link from city where population > $bigLimit"
      .query[City]
      .list

    runAndLogResults("All city names with population over 4M", program)
  }

  def selectMetroSystemsWithCityNames(): Unit = {
    case class MetroSystemWithCity(metroSystemName: String, cityName: String, dailyRidership: Int)

    val program = sql"select ms.name, c.name, ms.daily_ridership from metro_system as ms left join city as c on ms.city_id = c.id"
      .query[MetroSystemWithCity]
      .list

    runAndLogResults("Metro systems with city names", program)
  }

  def checkQuery(): Unit = {
    println("Analyzing query for correctness")

    import xa.yolo._
    sql"select name from city".query[String].check.unsafePerformIO
    
    println()
  }

  def runAndLogResults[R](label: String, program: ConnectionIO[List[R]]): Unit = {
    println(label)
    program.transact(xa).unsafePerformIO.foreach(println)
    println()
  }

  insertWithGeneratedId()
  selectAll()
  selectNamesOfBig()
  selectMetroSystemsWithCityNames()
  checkQuery()
}
