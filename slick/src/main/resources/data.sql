INSERT INTO city(id, name, population, area, link) VALUES
    (1, 'Warszawa', 1748916, 517.24, 'http://www.um.warszawa.pl/en'),
    (2, 'Paris', 2243833, 105.4, 'http://paris.fr'),
    (3, 'Chongqing', 49165500, 82403, NULL) RETURNING id;

ALTER SEQUENCE city_id_seq RESTART WITH 4;

INSERT INTO metro_system(id, city_id, name, daily_ridership) VALUES
    (10, 1, 'Metro Warszawskie', 568000),
    (20, 2, 'Métro de Paris', 4160000),
    (30, 3, 'Chongqing Metro', 1730000);

ALTER SEQUENCE metro_system_id_seq RESTART WITH 40;

INSERT INTO metro_line(id, system_id, name, station_count, track_type) VALUES
    (100, 10, 'M1', 21, 1),
    (101, 10, 'M2', 7, 1),
    (200, 20, '1', 25, 3),
    (201, 20, '2', 25, 1),
    (202, 20, '3', 25, 1),
    (203, 20, '3bis', 4, 1),
    (204, 20, '4', 27, 3),
    (205, 20, '5', 22, 1),
    (206, 20, '6', 28, 3),
    (207, 20, '7', 38, 1),
    (208, 20, '7bis', 8, 1),
    (209, 20, '8', 38, 1),
    (210, 20, '9', 37, 1),
    (211, 20, '10', 23, 1),
    (212, 20, '11', 13, 3),
    (213, 20, '12', 29, 1),
    (214, 20, '13', 32, 1),
    (215, 20, '14', 9, 3),
    (300, 30, '1', 23, 1),
    (301, 30, '2', 25, 2),
    (302, 30, '3', 45, 2),
    (303, 30, '6', 33, 1);

ALTER SEQUENCE metro_line_id_seq RESTART WITH 400;
