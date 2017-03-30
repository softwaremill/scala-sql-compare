DROP TABLE IF EXISTS "metro_line";
DROP TABLE IF EXISTS "metro_system";
DROP TABLE IF EXISTS "city";

CREATE TABLE "city"(
    "id" BIGINT NOT NULL,
    "name" VARCHAR NOT NULL,
    "population" BIGINT NOT NULL,
    "area" FLOAT NOT NULL,
    "link" VARCHAR
);
ALTER TABLE "city" ADD CONSTRAINT "city_id" PRIMARY KEY("id");

CREATE TABLE "metro_system"(
    "id" BIGINT NOT NULL,
    "city_id" BIGINT NOT NULL,
    "name" VARCHAR NOT NULL,
    "daily_ridership" BIGINT NOT NULL
);
ALTER TABLE "metro_system" ADD CONSTRAINT "metro_system_id" PRIMARY KEY("id");
ALTER TABLE "metro_system" ADD CONSTRAINT "metro_system_city_fk"
  FOREIGN KEY("city_id") REFERENCES "city"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "metro_line"(
    "id" BIGINT NOT NULL,
    "system_id" BIGINT NOT NULL,
    "name" VARCHAR NOT NULL,
    "station_count" BIGINT NOT NULL,
    "track_type" INT NOT NULL
);
ALTER TABLE "metro_line" ADD CONSTRAINT "metro_line_id" PRIMARY KEY("id");
ALTER TABLE "metro_line" ADD CONSTRAINT "metro_line_system_fk"
  FOREIGN KEY("system_id") REFERENCES "metro_system"("id") ON DELETE CASCADE ON UPDATE CASCADE;