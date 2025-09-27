------------------------------------------- Create extentions ----------------------------------------------------------
CREATE EXTENSION postgis;
CREATE EXTENSION btree_gist;
----------------------------------------- Create Schemas ---------------------------------------------------------------
CREATE SCHEMA navigation_data;
CREATE SCHEMA geographical_data;
CREATE SCHEMA weather_data;
CREATE SCHEMA meta_data;
----------------------------------------- Create tables for Navigation Related data ------------------------------------
CREATE TABLE navigation_data.ais_codes_types (
    type_code DECIMAL(4,1) PRIMARY KEY,
    description TEXT NOT NULL);

CREATE TABLE navigation_data.ais_static_saronic (
    vessel_id VARCHAR(64) PRIMARY KEY,
    country VARCHAR(50),             -- Full country name 
    shiptype DECIMAL(4,1),           -- Ship type code 
    FOREIGN KEY (shiptype) REFERENCES navigation_data.ais_codes_types(type_code));

CREATE TABLE navigation_data.ais_kinematic_saronic (
    ID INTEGER GENERATED ALWAYS AS IDENTITY ,
    t BIGINT NOT NULL,                        -- UNIX timestamp
    vessel_id VARCHAR(64),
    lon DOUBLE PRECISION,                     -- WGS84 longitude 
    lat DOUBLE PRECISION,                     -- WGS84 latitude 
    heading  DOUBLE PRECISION,                -- True heading 0-359 degrees 
    speed DOUBLE PRECISION,                   -- Speed over ground in knots 
    course DOUBLE PRECISION,                  -- Course over ground 
	datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,  -- actual timestamp used for partitioning
	PRIMARY KEY (ID, datetime),
    FOREIGN KEY (vessel_id) REFERENCES navigation_data.ais_static_saronic(vessel_id)
)PARTITION BY RANGE (datetime);

ALTER TABLE navigation_data.ais_kinematic_saronic
ADD CONSTRAINT unique_vessel_time UNIQUE (vessel_id, datetime);

CREATE TABLE navigation_data.ais_kinematic_saronic_month1
PARTITION OF navigation_data.ais_kinematic_saronic 
FOR VALUES FROM ('2018-04-30') TO ('2018-05-31 23:59:59');

CREATE TABLE navigation_data.ais_kinematic_saronic_month2
PARTITION OF navigation_data.ais_kinematic_saronic 
FOR VALUES FROM ('2018-06-01') TO ('2018-06-30 23:59:59');

CREATE TABLE navigation_data.ais_kinematic_saronic_month3
PARTITION OF navigation_data.ais_kinematic_saronic 
FOR VALUES FROM ('2018-07-01') TO ('2018-07-31 23:59:59');

CREATE TABLE navigation_data.ais_kinematic_saronic_month4
PARTITION OF navigation_data.ais_kinematic_saronic 
FOR VALUES FROM ('2018-08-01') TO ('2018-08-31 23:59:59');

CREATE TABLE navigation_data.ais_kinematic_saronic_month5
PARTITION OF navigation_data.ais_kinematic_saronic 
FOR VALUES FROM ('2018-09-01') TO ('2018-09-30 23:59:59');

CREATE TABLE navigation_data.ais_kinematic_saronic_month6
PARTITION OF navigation_data.ais_kinematic_saronic 
FOR VALUES FROM ('2018-10-01') TO ('2018-10-31 23:59:59');

CREATE TABLE navigation_data.ais_kinematic_saronic_default
PARTITION OF navigation_data.ais_kinematic_saronic DEFAULT;
------------------------------------------------ Create table for Meta Data ---------------------------------------------
CREATE TABLE meta_data.trajectory_synopses (
    ID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    t BIGINT NOT NULL,
    vessel_id VARCHAR(64),
    annotations TEXT,                        -- Semantic annotations 
    transport_trail TEXT,                    -- Trajectory segment information
	datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	FOREIGN KEY (vessel_id, datetime) REFERENCES navigation_data.ais_kinematic_saronic(vessel_id, datetime));
---------------------------------------- Load tables for Navigation Related data ----------------------------------------
COPY navigation_data.ais_codes_types
FROM '/AIS_Data/ais_codes_descriptions.csv' 
DELIMITER ',' 
CSV HEADER;

COPY navigation_data.ais_static_saronic
FROM '/AIS_Data/unipi_ais_static_new.csv' 
DELIMITER ',' 
CSV HEADER;

COPY navigation_data.ais_kinematic_saronic
	(t, vessel_id, lon, lat, heading, speed, course, datetime)
FROM '/AIS_Data/unipi_ais_dynamic_new.csv' 
DELIMITER ',' 
CSV HEADER;
------------------------------------------- Load table for Meta Data ----------------------------------------------------
COPY meta_data.trajectory_synopses
	(t, vessel_id, annotations, transport_trail,datetime)
FROM '/AIS_Data/unipi_ais_synopses_new.csv' 
DELIMITER ',' 
CSV HEADER;
----------------------------------------------- Modify the tables -------------------------------------------------------
ALTER TABLE geographical_data.antenna_coverage          
DROP COLUMN gid;

ALTER TABLE geographical_data.antenna_coverage   
DROP COLUMN population;

ALTER TABLE geographical_data.antenna_coverage
ADD CONSTRAINT antenna_coverage_pkey PRIMARY KEY (area_id);

ALTER TABLE geographical_data.islands          
DROP COLUMN gid;

ALTER TABLE geographical_data.islands      
ADD CONSTRAINT islands_pkey PRIMARY KEY (fid);

ALTER TABLE geographical_data.piraeus_port        
DROP COLUMN gid;

ALTER TABLE geographical_data.piraeus_port      
ADD CONSTRAINT piraeus_port_pkey PRIMARY KEY (fid);

ALTER TABLE geographical_data.receiver_location        
DROP COLUMN gid;

ALTER TABLE geographical_data.receiver_location      
ADD CONSTRAINT receiver_location_pkey PRIMARY KEY (fid);

ALTER TABLE geographical_data.territorial_waters        
DROP COLUMN gid;

ALTER TABLE geographical_data.territorial_waters       
ADD CONSTRAINT territorial_water_pkey PRIMARY KEY (fid);

ALTER TABLE navigation_data.ais_kinematic_saronic
ADD COLUMN geom geometry(Point, 4326);

UPDATE navigation_data.ais_kinematic_saronic
SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326)
WHERE lon IS NOT NULL AND lat IS NOT NULL;

UPDATE geographical_data.antenna_coverage
SET geom = ST_SetSRID(geom, 4326)
WHERE ST_SRID(geom) = 0;

UPDATE weather_data.noaa_weather 
SET geom = ST_SetSRID(geom, 4326)
WHERE ST_SRID(geom) = 0;
------------------------------------------- Creating Indexes ------------------------------------------------------------
--Indexes for spatial queries
CREATE INDEX idx_kinematic_geom
ON navigation_data.ais_kinematic_saronic
USING GIST (geom);

CREATE INDEX idx_weather_geom
ON weather_data.noaa_weather
USING GIST (geom);

CREATE INDEX idx_antenna_geom
ON geographical_data.antenna_coverage
USING GIST (geom);

CREATE INDEX idx_port_geom
ON geographical_data.piraeus_port
USING GIST (geom);

CREATE INDEX idx_territorial_geom
ON geographical_data.territorial_waters
USING GIST (geom);

--Indexes for temporal queries
CREATE INDEX idx_kinematic_datetime 
ON navigation_data.ais_kinematic_saronic
USING BRIN (datetime);

--Composite Indexes 
CREATE INDEX idx_kinematic_vessel_datetime_include_geom
ON navigation_data.ais_kinematic_saronic (vessel_id, datetime) INCLUDE (geom);

CREATE INDEX idx_trajectory_synopses_tvessel 
ON meta_data.trajectory_synopses(t, vessel_id);
------------------------------------------- Q1 Position Query -----------------------------------------------------------
-- 100 ships
SELECT DISTINCT ON (vessel_id)
    vessel_id, datetime, geom
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
    FROM navigation_data.ais_kinematic_saronic
    LIMIT 100)
ORDER BY vessel_id, datetime;

-- 1000 ships
SELECT DISTINCT ON (vessel_id)
    vessel_id, datetime, geom
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
    FROM navigation_data.ais_kinematic_saronic
    LIMIT 1000)
ORDER BY vessel_id, datetime;

-- 3000 ships
SELECT DISTINCT ON (vessel_id)
    vessel_id, datetime, geom
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
    FROM navigation_data.ais_kinematic_saronic
    LIMIT 3000)
ORDER BY vessel_id, datetime;
------------------------------------------- Q2 Trajectory Query ----------------------------------------------------------
-- 1 month, 1000 ships
SELECT vessel_id, ST_MakeLine(geom ORDER BY datetime) AS trajectory
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
  SELECT DISTINCT vessel_id
  FROM navigation_data.ais_kinematic_saronic
  WHERE datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
  LIMIT 1000)
AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
GROUP BY vessel_id
ORDER BY vessel_id;

-- 3 months, 1000 ships
SELECT vessel_id, ST_MakeLine(geom ORDER BY datetime) AS trajectory
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
  SELECT DISTINCT vessel_id
  FROM navigation_data.ais_kinematic_saronic
  WHERE datetime BETWEEN '2018-05-01 00:00:00' AND '2018-07-31 23:59:59'
  LIMIT 1000)
AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-07-31 23:59:59'
GROUP BY vessel_id
ORDER BY vessel_id;

-- 6 months, 1000 ships
SELECT vessel_id, ST_MakeLine(geom ORDER BY datetime) AS trajectory
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
  SELECT DISTINCT vessel_id
  FROM navigation_data.ais_kinematic_saronic
  WHERE datetime BETWEEN '2018-05-01 00:00:00' AND '2018-10-31 23:59:59'
  LIMIT 1000)
AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-10-31 23:59:59'
GROUP BY vessel_id
ORDER BY vessel_id;
------------------------------------------- Q3 Proximity Query -----------------------------------------------------------
-- 1km from Piraeus, 1000 ships
SELECT DISTINCT ON (vessel_id) vessel_id, datetime, geom
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
	WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326)::geography,
	1000)
	LIMIT 1000)
AND ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326)::geography,
    1000);

-- 10km from Piraeus, 1000 ships
SELECT DISTINCT ON (vessel_id) vessel_id, datetime, geom
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
	WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326)::geography,
	10000)
	LIMIT 1000)
AND ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326)::geography,
    10000);

-- 50km from Piraeus, 1000 ships
SELECT DISTINCT ON (vessel_id) vessel_id, datetime, geom
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
	WHERE ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326)::geography,
	50000)
	LIMIT 1000)
AND ST_DWithin(
    geom::geography,
    ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326)::geography,
    50000);
------------------------------------------- Q4 Polygon Query ------------------------------------------------------------
-- Small Polygon - Piraeus Port Area, 1000 ships
SELECT DISTINCT ON (vessel_id) vessel_id, geom, datetime as timestamp
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
	WHERE ST_Within(geom, ST_GeomFromText('POLYGON((23.62 37.93, 23.67 37.93, 23.67 37.97, 23.62 37.97, 23.62 37.93))', 4326))
	LIMIT 1000)
AND ST_Within(geom, ST_GeomFromText('POLYGON((23.62 37.93, 23.67 37.93, 23.67 37.97, 23.62 37.97, 23.62 37.93))', 4326));

-- Medium Polygon - Central Saronic Gulf, 1000 ships
SELECT DISTINCT ON (vessel_id) vessel_id, geom, datetime as timestamp
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
	WHERE ST_Within(geom, ST_GeomFromText('POLYGON((23.45 37.85, 23.75 37.85, 23.75 38.05, 23.45 38.05, 23.45 37.85))', 4326))
	LIMIT 1000)
AND ST_Within(geom, ST_GeomFromText('POLYGON((23.45 37.85, 23.75 37.85, 23.75 38.05, 23.45 38.05, 23.45 37.85))', 4326));

-- Large Polygon - Extended Saronic Gulf, 1000 ships
SELECT DISTINCT ON (vessel_id) vessel_id, geom, datetime as timestamp
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
	WHERE ST_Within(geom, ST_GeomFromText('POLYGON((23.25 37.70, 23.85 37.70, 23.85 38.15, 23.25 38.15, 23.25 37.70))', 4326))
	LIMIT 1000)
AND ST_Within(geom, ST_GeomFromText('POLYGON((23.25 37.70, 23.85 37.70, 23.85 38.15, 23.25 38.15, 23.25 37.70))', 4326));
------------------------------------------- Q5 Range Query -------------------------------------------------------------
-- 1 month, small bouding box
SELECT vessel_id, geom, datetime
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
    WHERE geom && ST_MakeEnvelope(23.675, 37.925, 23.725, 37.975, 4326) 
	AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
	LIMIT 1000)
AND geom && ST_MakeEnvelope(23.675, 37.925, 23.725, 37.975, 4326) 
AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59';

-- 3 months, medium bouding box
SELECT vessel_id, geom, datetime
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
    WHERE geom && ST_MakeEnvelope(23.65, 37.9, 23.75, 38.0, 4326) 
	AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-07-31 23:59:59'
	LIMIT 1000)
AND geom && ST_MakeEnvelope(23.65, 37.9, 23.75, 38.0, 4326) 
AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-07-31 23:59:59';
	
-- 6 month, large bouding box
SELECT vessel_id, geom, datetime
FROM navigation_data.ais_kinematic_saronic
WHERE vessel_id IN (
    SELECT DISTINCT vessel_id
	FROM navigation_data.ais_kinematic_saronic 
    WHERE geom && ST_MakeEnvelope(23.5, 37.8, 24.0, 38.3, 4326) 
	AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-10-31 23:59:59'
	LIMIT 1000)
AND geom && ST_MakeEnvelope(23.5, 37.8, 24.0, 38.3, 4326) 
AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-10-31 23:59:59';
------------------------------------------- Q6 Distance Join -------------------------------------------------------------
-- 1km, 1 month, 1000 ships
WITH candidate_vessels AS (
  SELECT DISTINCT vessel_id, geom
  FROM navigation_data.ais_kinematic_saronic_month1
  LIMIT 1000)
SELECT 
  a.vessel_id AS vessel_a,
  b.vessel_id AS vessel_b,
  a.datetime,
  ST_Distance(
    ST_Transform(a.geom, 3857),  -- convert to meters
    ST_Transform(b.geom, 3857)) AS distance_meters
FROM navigation_data.ais_kinematic_saronic_month1 a
JOIN navigation_data.ais_kinematic_saronic_month1 b
  ON a.datetime = b.datetime
 AND a.vessel_id < b.vessel_id
JOIN candidate_vessels va ON a.vessel_id = va.vessel_id
JOIN candidate_vessels vb ON b.vessel_id = vb.vessel_id
WHERE ST_DWithin(
  ST_Transform(a.geom, 3857),
  ST_Transform(b.geom, 3857),
  1000.0);  -- meters   
  
-- 2km, 1 month, 1000 ships 
WITH candidate_vessels AS (
  SELECT DISTINCT vessel_id
  FROM navigation_data.ais_kinematic_saronic_month1
  LIMIT 1000)
SELECT 
  a.vessel_id AS vessel_a,
  b.vessel_id AS vessel_b,
  a.datetime,
  ST_Distance(
    ST_Transform(a.geom, 3857),  -- convert to meters
    ST_Transform(b.geom, 3857)) AS distance_meters
FROM navigation_data.ais_kinematic_saronic_month1 a
JOIN navigation_data.ais_kinematic_saronic_month1 b
  ON a.datetime = b.datetime
 AND a.vessel_id < b.vessel_id
JOIN candidate_vessels va ON a.vessel_id = va.vessel_id
JOIN candidate_vessels vb ON b.vessel_id = vb.vessel_id
WHERE ST_DWithin(
  ST_Transform(a.geom, 3857),
  ST_Transform(b.geom, 3857),
  2000 ); -- meters   

-- 3km, 1 month, 1000 ships 
WITH candidate_vessels AS (
  SELECT DISTINCT vessel_id
  FROM navigation_data.ais_kinematic_saronic_month1
  LIMIT 1000)
SELECT 
  a.vessel_id AS vessel_a,
  b.vessel_id AS vessel_b,
  a.datetime,
  ST_Distance(
    ST_Transform(a.geom, 3857),  -- convert to meters
    ST_Transform(b.geom, 3857)) AS distance_meters
FROM navigation_data.ais_kinematic_saronic_month1 a
JOIN navigation_data.ais_kinematic_saronic_month1 b
  ON a.datetime = b.datetime
 AND a.vessel_id < b.vessel_id
JOIN candidate_vessels va ON a.vessel_id = va.vessel_id
JOIN candidate_vessels vb ON b.vessel_id = vb.vessel_id
WHERE ST_DWithin(
  ST_Transform(a.geom, 3857),
  ST_Transform(b.geom, 3857),
  3000 ); -- meters   
------------------------------------------- Q7 Nearest Neighbor -------------------------------------------------------------
-- 100 ships, 1 month
SELECT DISTINCT ON (vessel_id) vessel_id, datetime,geom, 
       ST_Distance(
         ST_Transform(geom, 3857), 
         ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)) AS distance_meters
FROM navigation_data.ais_kinematic_saronic
WHERE datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
ORDER BY vessel_id,ST_Transform(geom, 3857) <-> ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)
LIMIT 100

-- 1000 ships, 1 month
SELECT DISTINCT ON (vessel_id) vessel_id, datetime, geom,
       ST_Distance(
         ST_Transform(geom, 3857), 
         ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)) AS distance_meters
FROM navigation_data.ais_kinematic_saronic
WHERE datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
ORDER BY vessel_id,ST_Transform(geom, 3857) <-> ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)
LIMIT 1000

-- 3000 ships, 1 month
SELECT DISTINCT ON (vessel_id) vessel_id, datetime, geom,
       ST_Distance(
         ST_Transform(geom, 3857), 
         ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)) AS distance_meters
FROM navigation_data.ais_kinematic_saronic
WHERE datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
ORDER BY vessel_id,ST_Transform(geom, 3857) <-> ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)
LIMIT 3000
------------------------------------------- Q8 Clustering -------------------------------------------------------------
--Small Polygon - Piraeus Port Area, 50 meters, 1 month
SELECT cluster_id, ST_Centroid(ST_Collect(geom)) AS cluster_geom
FROM (
  SELECT k.geom AS geom,
         ST_ClusterDBSCAN(ST_Transform(k.geom, 3857), 25, 3) OVER () AS cluster_id
  FROM meta_data.trajectory_synopses s
  JOIN navigation_data.ais_kinematic_saronic k
    ON s.vessel_id = k.vessel_id AND s.t = k.t
  WHERE k.datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
    AND ST_Within(k.geom, (SELECT geom FROM geographical_data.piraeus_port))
) AS clustered
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id;

-- Medium Polygon - Central Saronic Gulf, 500 meters, 1 month
SELECT cluster_id, ST_Centroid(ST_Collect(geom)) AS cluster_geom
FROM (
  SELECT k.geom AS geom,
         ST_ClusterDBSCAN(ST_Transform(k.geom, 3857), 500, 3) OVER () AS cluster_id
  FROM meta_data.trajectory_synopses s
  JOIN navigation_data.ais_kinematic_saronic k
    ON s.vessel_id = k.vessel_id AND s.t = k.t
  WHERE k.datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
    AND ST_Within(geom, ST_GeomFromText('POLYGON((23.45 37.85, 23.75 37.85, 23.75 38.05, 23.45 38.05, 23.45 37.85))', 4326))
) AS clustered
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id;

-- Large Polygon - Extended Saronic Gulf, 500 meters, 1 month
SELECT cluster_id, ST_Centroid(ST_Collect(geom)) AS cluster_geom
FROM (
  SELECT k.geom AS geom,
         ST_ClusterDBSCAN(ST_Transform(k.geom, 3857), 500, 3) OVER () AS cluster_id
  FROM meta_data.trajectory_synopses s
  JOIN navigation_data.ais_kinematic_saronic k
    ON s.vessel_id = k.vessel_id AND s.t = k.t
  WHERE k.datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59'
    AND ST_Within(k.geom, (SELECT geom FROM geographical_data.territorial_waters))
) AS clustered
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id;
------------------------------------------- Q9 Intersect Join ----------------------------------------------------------
-- 20 areas, 1 month
WITH limited_zones AS (
  SELECT * 
  FROM geographical_data.antenna_coverage
  ORDER BY area_id 
  LIMIT 20 ),
intersections AS (
  SELECT 
    a.vessel_id,
    a.datetime,
    z.area_id,
    a.geom
  FROM navigation_data.ais_kinematic_saronic a
  JOIN limited_zones z
    ON ST_Intersects(a.geom, z.geom)
  WHERE a.datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59')
SELECT DISTINCT ON (vessel_id, area_id)
  vessel_id, datetime, area_id, geom
FROM intersections
ORDER BY vessel_id, area_id, datetime;

-- 50 areas, 1 month
WITH limited_zones AS (
  SELECT * 
  FROM geographical_data.antenna_coverage
  ORDER BY area_id 
  LIMIT 50 ),
intersections AS (
  SELECT 
    a.vessel_id,
    a.datetime,
    z.area_id,
    a.geom
  FROM navigation_data.ais_kinematic_saronic a
  JOIN limited_zones z
    ON ST_Intersects(a.geom, z.geom)
  WHERE a.datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59')
SELECT DISTINCT ON (vessel_id, area_id)
  vessel_id, datetime, area_id, geom
FROM intersections
ORDER BY vessel_id, area_id, datetime;

-- all areas, 1 month
SELECT DISTINCT ON (k.vessel_id, r.area_id) k.vessel_id, k.datetime, r.area_id, k.geom
FROM navigation_data.ais_kinematic_saronic k
JOIN geographical_data.antenna_coverage r
  ON ST_Intersects(k.geom, r.geom)
WHERE k.datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-31 23:59:59';
------------------------------------------- Q10 Weather Related -------------------------------------------------------
-- Ships sailing during stormy winds
SELECT vessel_id, datetime, speed, k.geom
FROM navigation_data.ais_kinematic_saronic k
JOIN weather_data.noaa_weather w
  ON ST_DWithin(k.geom::geography, w.geom::geography, 7000) -- 7km radius
  AND k.datetime BETWEEN w.timestamp::timestamp - INTERVAL '30 minutes'
                   AND w.timestamp::timestamp + INTERVAL '30 minutes'
WHERE w.WSPD >20;  -- Gale force or higher

--  Wind gusts with sudden speed drops in a year
SELECT k.vessel_id, k.datetime, k.speed, w.GUST
FROM navigation_data.ais_kinematic_saronic k
JOIN weather_data.noaa_weather w
  ON ST_DWithin(k.geom::geography, w.geom::geography, 7000)
  AND k.datetime BETWEEN w.timestamp::timestamp - INTERVAL '30 minutes'
                   AND w.timestamp::timestamp + INTERVAL '30 minutes'
WHERE w.GUST > 30 AND k.speed < 5 
AND k.datetime BETWEEN '2018-05-01 00:00:00' AND '2019-04-31 23:59:59'
ORDER BY k.datetime;
------------------------------------------- Q11 Semantics -------------------------------------------------------------
-- STOP_START annotations
SELECT 
  s.vessel_id,
  s.datetime AS annotated_time,
  k.geom,
  k.speed,
  k.heading,
  k.course
FROM meta_data.trajectory_synopses s
JOIN navigation_data.ais_kinematic_saronic k
  ON s.vessel_id = k.vessel_id
 AND s.t = k.t
WHERE s.annotations ILIKE '%STOP_START%'
  AND k.geom IS NOT NULL;
  
-- High-Density Routes based on STOP_START annotations in a day
SELECT 
    s.vessel_id,
    s.datetime AS annotated_time,
    ST_MakeLine(k.geom ORDER BY k.datetime) AS trail_geom
FROM meta_data.trajectory_synopses s
JOIN navigation_data.ais_kinematic_saronic k
ON s.vessel_id = k.vessel_id
WHERE s.annotations ILIKE '%STOP_START%'
AND s.datetime BETWEEN '2018-05-01 00:00:00' AND '2018-05-01 23:59:59'
AND k.datetime BETWEEN s.datetime - INTERVAL '30 minutes'
AND s.datetime + INTERVAL '30 minutes'
AND k.geom IS NOT NULL
GROUP BY s.vessel_id, s.datetime;


























