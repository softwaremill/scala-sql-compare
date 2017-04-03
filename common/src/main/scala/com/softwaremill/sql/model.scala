package com.softwaremill.sql

import com.softwaremill.sql.TrackType.TrackType

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