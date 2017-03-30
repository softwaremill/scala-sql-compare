SELECT name, population FROM city WHERE population > 2000000;

SELECT ms.name, c.name, ms.daily_ridership
    FROM metro_system as ms
    LEFT JOIN city AS c ON ms.city_id = c.id
    ORDER BY ms.daily_ridership DESC;

SELECT ml.name, ms.name, c.name, ml.station_count
    FROM metro_line as ml
    LEFT JOIN metro_system as ms on ml.system_id = ms.id
    LEFT JOIN city AS c ON ms.city_id = c.id
    ORDER BY ml.station_count DESC;

SELECT ms.name, c.name, COUNT(ml.id) as line_count
    FROM metro_line as ml
    LEFT JOIN metro_system as ms on ml.system_id = ms.id
    LEFT JOIN city AS c ON ms.city_id = c.id
    GROUP BY ms.id, c.id
    ORDER BY line_count DESC;