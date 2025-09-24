------------------------------------------- Create extentions --------------------------------------------------------
CREATE EXTENSION citus;
CREATE EXTENSION postgis;
CREATE EXTENSION mobilitydb;
CREATE EXTENSION distributed_mobilitydb;
------------------------------------------- Add worker nodes ---------------------------------------------------------
SELECT * FROM citus_add_node('worker1', 5432);
SELECT * FROM citus_add_node('worker2', 5432);
----------------------------------------- Create Schemas --------------------------------------------------------------
CREATE SCHEMA navigation_data;
CREATE SCHEMA geographical_data;
CREATE SCHEMA weather_data;
CREATE SCHEMA meta_data;
---------------------------------------- Create tables for Navigation Related data ------------------------------------
CREATE TABLE navigation_data.ais_codes_types (
    type_code DECIMAL(4,1) PRIMARY KEY,
    description TEXT NOT NULL);

CREATE TABLE navigation_data.ais_static_saronic (
    vessel_id VARCHAR(64) PRIMARY KEY,
    country VARCHAR(50),             -- Full country name 
    shiptype DECIMAL(4,1),           -- Ship type code 
    FOREIGN KEY (shiptype) REFERENCES navigation_data.ais_codes_types(type_code));

CREATE TABLE navigation_data.ais_kinematic_saronic (
    ID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    t BIGINT NOT NULL,                        -- UNIX timestamp
    vessel_id VARCHAR(64),
    lon DOUBLE PRECISION,                     -- WGS84 longitude 
    lat DOUBLE PRECISION,                     -- WGS84 latitude 
    heading  DOUBLE PRECISION,                -- True heading 0-359 degrees 
    speed DOUBLE PRECISION,                   -- Speed over ground in knots 
    course DOUBLE PRECISION,                  -- Course over ground 
	datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    FOREIGN KEY (vessel_id) REFERENCES navigation_data.ais_static_saronic(vessel_id));

ALTER TABLE navigation_data.ais_kinematic_saronic
ADD CONSTRAINT unique_vessel_time UNIQUE (vessel_id, datetime);
------------------------------------------------ Create table for Meta Data ---------------------------------------------
CREATE TABLE meta_data.trajectory_synopses (
    ID INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    t BIGINT NOT NULL,
    vessel_id VARCHAR(64),
    annotations TEXT,                        -- Semantic annotations 
    transport_trail TEXT,                    -- Trajectory segment information
	datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	FOREIGN KEY (vessel_id, datetime) REFERENCES navigation_data.ais_kinematic_saronic(vessel_id, datetime));
---------------------------------------- Load tables for Navigation Related data -----------------------------------------
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
------------------------------------------- Load table for Meta Data -------------------------------------------------------
COPY meta_data.trajectory_synopses
	(t, vessel_id, annotations, transport_trail,datetime)
FROM '/AIS_Data/unipi_ais_synopses_new.csv' 
DELIMITER ',' 
CSV HEADER;
----------------------------------------------- Modify the tables -----------------------------------------------------------
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

ALTER TABLE navigation_data.ais_kinematic_saronic
ADD COLUMN tgeom tgeompoint;

UPDATE navigation_data.ais_kinematic_saronic
SET tgeom = tgeompointinst(tgeompoint(geom::geometry, datetime::timestamp));

UPDATE geographical_data.antenna_coverage
SET geom = ST_SetSRID(geom, 4326)
WHERE ST_SRID(geom) = 0;

UPDATE weather_data.noaa_weather
SET geom = ST_SetSRID(geom, 4326)
WHERE ST_SRID(geom) = 0;
------------------------------------------- Create new trajectory tables ---------------------------------------------------------
CREATE TABLE navigation_data.vessel_trajectories (
    vessel_id VARCHAR(64) PRIMARY KEY,
    traj   tgeompoint,                   -- Full vessel trajectory as a MobilityDB temporal geometry
    bbox stbox,                         -- Spatiotemporal bounding box (for fast coarse filtering)
    start_time TIMESTAMP WITHOUT TIME ZONE, -- Trajectory start
    end_time TIMESTAMP WITHOUT TIME ZONE,   -- Trajectory end
    duration INTERVAL,                  -- Total time duration
    length DOUBLE PRECISION,            -- Total trajectory length (meters)
    avg_speed DOUBLE PRECISION,         -- Average speed (computed as length/duration)
    num_instants INTEGER                -- Number of AIS points in trajectory   
);


CREATE TABLE meta_data.trajectory_synopses_mobilitydb AS
SELECT s.id, s.t, s.vessel_id ,s.annotations, s.transport_trail, s.datetime,
-- Trajectory as tgeompointseq
  tgeompointseq(array_agg(tgeompoint(ST_SetSRID(k.geom, 4326), k.datetime::timestamptz)ORDER BY k.datetime) FILTER (WHERE k.geom IS NOT NULL), 
    'Linear', true, true) AS traj,
-- Speed as tfloatseq
  tfloatseq(array_agg(tfloat(k.speed, k.datetime::timestamptz)ORDER BY k.datetime ) FILTER (WHERE k.speed IS NOT NULL),
    'Linear', true, true) AS speed,
-- Heading as tfloatseq
  tfloatseq(array_agg(tfloat(k.heading, k.datetime::timestamptz)ORDER BY k.datetime) FILTER (WHERE k.heading IS NOT NULL),
    'Linear', true, true) AS heading,
-- Course as tfloatseq  
  tfloatseq(array_agg(tfloat(k.course, k.datetime::timestamptz)ORDER BY k.datetime) FILTER (WHERE k.course IS NOT NULL),
  'Linear', true, true) AS course
FROM meta_data.trajectory_synopses s
JOIN navigation_data.ais_kinematic_saronic k 
   ON s.vessel_id = k.vessel_id
   AND s.t = k.t
WHERE k.geom IS NOT NULL
GROUP BY s.id, s.t, s.vessel_id, s.annotations, s.transport_trail, s.datetime;
------------------------------------------- Populate the vessel_trajectories table -------------------------------------------------
WITH vessel_data AS (
  SELECT 
    vessel_id,
    tgeompointseq(
      array_agg(tgeompoint(ST_SetSRID(geom, 4326), datetime::timestamptz) ORDER BY datetime),
      'Linear', true, true) AS traj
  FROM navigation_data.ais_kinematic_saronic
  WHERE geom IS NOT NULL
  GROUP BY vessel_id)
INSERT INTO navigation_data.vessel_trajectories (
    vessel_id, traj, bbox, start_time, end_time,
    duration, length, avg_speed, num_instants)
SELECT vessel_id, traj, stbox(traj), startTimestamp(traj), endTimestamp(traj), duration(traj), length(traj),
    CASE
        WHEN EXTRACT(EPOCH FROM duration(traj)) > 0
        THEN length(traj) / EXTRACT(EPOCH FROM duration(traj)) *3600
        ELSE NULL
    END,
    numInstants(traj)
FROM vessel_data;
------------------------------------------- Creating Referenced tables ------------------------------------------------------------
SELECT create_reference_table('geographical_data.piraeus_port');
SELECT create_reference_table('geographical_data.territorial_waters');
SELECT create_reference_table('geographical_data.antenna_coverage');
------------------------------------------- Creating Distributed tables ------------------------------------------------------------
SELECT create_distributed_table('navigation_data.vessel_trajectories', 'vessel_id');
SELECT create_distributed_table('meta_data.trajectory_synopses_mobilitydb', 'vessel_id');
------------------------------------------- Colocation Check -----------------------------------------------------------------------
SELECT c.relname AS table_name, p.colocationid AS colocation_id
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_dist_partition p ON p.logicalrelid = c.oid
WHERE c.relname IN ('vessel_trajectories','trajectory_synopses_mobilitydb');
------------------------------------------- Creating Indexes -----------------------------------------------------------------------
--Plain B-tree Indexes
CREATE INDEX idx_vessel_trajectories_vessel_id
ON navigation_data.vessel_trajectories (vessel_id);

CREATE INDEX idx_synopses_mobilitydb_vessel_id
ON meta_data.trajectory_synopses_mobilitydb (vessel_id);

--Index for spatial queries for geom
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

--Index for temporal queries           
CREATE INDEX idx_trajectories_start_time
ON navigation_data.vessel_trajectories (start_time);

CREATE INDEX idx_trajectories_end_time
ON navigation_data.vessel_trajectories (end_time);

--Index for spatial temporal queries 
CREATE INDEX idx_kinematic_tgeom
ON navigation_data.ais_kinematic_saronic
USING GIST (tgeom);

CREATE INDEX idx_vessel_trajectories_traj
ON navigation_data.vessel_trajectories
USING GIST (traj);

CREATE INDEX idx_vessel_trajectories_bbox
ON navigation_data.vessel_trajectories
USING GIST (bbox);

--Composite Indexes 
CREATE INDEX idx_synopses_mobilitydb_vessel_time
ON meta_data.trajectory_synopses_mobilitydb (vessel_id, datetime);
------------------------------------------- Q1 Position Query -----------------------------------------------------------
-- 100 ships
SELECT 
    vessel_id,
    traj,
    startValue(traj) AS first_geom,
    startTimestamp(traj) AS first_time
FROM navigation_data.vessel_trajectories
ORDER BY vessel_id
LIMIT 100;

-- 1000 ships
SELECT 
    vessel_id,
    traj,
    startValue(traj) AS first_geom,
    startTimestamp(traj) AS first_time
FROM navigation_data.vessel_trajectories
ORDER BY vessel_id
LIMIT 1000;

-- 3000 ships
SELECT 
    vessel_id,
    traj,
    startValue(traj) AS first_geom,
    startTimestamp(traj) AS first_time
FROM navigation_data.vessel_trajectories
ORDER BY vessel_id
LIMIT 3000;
------------------------------------------- Q2 Trajectory Query ----------------------------------------------------------
-- 1 month, 1000 ships
SELECT
  vessel_id,
  attime(traj, '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan) AS traj_may
FROM navigation_data.vessel_trajectories
WHERE start_time <= '2018-05-31 23:59:59'
  AND end_time >= '2018-05-01 00:00:00'
ORDER BY vessel_id
LIMIT 1000;

-- 3 month, 1000 ships
SELECT
  vessel_id,
  attime(traj, '[2018-05-01 00:00:00+00, 2018-07-31 23:59:59+00]'::tstzspan) AS traj_may
FROM navigation_data.vessel_trajectories
WHERE start_time <= '2018-07-31 23:59:59'
  AND end_time >= '2018-05-01 00:00:00'
ORDER BY vessel_id
LIMIT 1000;

-- 6 month, 1000 ships
SELECT
  vessel_id,
  attime(traj, '[2018-05-01 00:00:00+00, 2018-10-31 23:59:59+00]'::tstzspan) AS traj_may
FROM navigation_data.vessel_trajectories
WHERE start_time <= '2018-10-31 23:59:59'
  AND end_time >= '2018-05-01 00:00:00'
ORDER BY vessel_id
LIMIT 1000;
------------------------------------------- Q3 Proximity Query -----------------------------------------------------------
-- 1km from Piraeus, 1000 ships
SELECT DISTINCT ON (vessel_id)   
  vessel_id,
  getValue(inst) AS geom
FROM (
  SELECT 
    vessel_id,
    unnest(Instants(traj)) AS inst
  FROM navigation_data.vessel_trajectories
  WHERE bbox && stbox(
    ST_Buffer(ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326), 0.01))
) sub
WHERE ST_DWithin(
  ST_Transform(getValue(inst), 3857),
  ST_Transform(ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326), 3857),
  1000 )-- meters
ORDER BY vessel_id
LIMIT 1000;

-- 10km from Piraeus, 1000 ships
SELECT DISTINCT ON (vessel_id)   
  vessel_id,
  getValue(inst) AS geom
FROM (
  SELECT 
    vessel_id,
    unnest(Instants(traj)) AS inst
  FROM navigation_data.vessel_trajectories
  WHERE bbox && stbox(
    ST_Buffer(ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326), 0.01))
) sub
WHERE ST_DWithin(
  ST_Transform(getValue(inst), 3857),
  ST_Transform(ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326), 3857),
  10000 )-- meters
ORDER BY vessel_id
LIMIT 1000;

-- 50km from Piraeus, 1000 ships
SELECT DISTINCT ON (vessel_id)   
  vessel_id,
  getValue(inst) AS geom
FROM (
  SELECT 
    vessel_id,
    unnest(Instants(traj)) AS inst
  FROM navigation_data.vessel_trajectories
  WHERE bbox && stbox(
    ST_Buffer(ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326), 0.01))
) sub
WHERE ST_DWithin(
  ST_Transform(getValue(inst), 3857),
  ST_Transform(ST_SetSRID(ST_MakePoint(23.6431, 37.9406), 4326), 3857),
  50000 )-- meters
ORDER BY vessel_id
LIMIT 1000;
------------------------------------------- Q4 Polygon Query ------------------------------------------------------------
-- Small Polygon - Piraeus Port Area, 1000 ships
SELECT DISTINCT ON (vessel_id)    
  vessel_id,
  getvalue(inst) AS geom,
  gettimestamp(inst) AS datetime
FROM (
  SELECT
    vessel_id,
    unnest(instants(traj)) AS inst
  FROM navigation_data.vessel_trajectories
  WHERE bbox && stbox(
    ST_GeomFromText('POLYGON((23.62 37.93, 23.67 37.93, 23.67 37.97, 23.62 37.97, 23.62 37.93))', 4326))
) sub
WHERE ST_DWithin(
  getvalue(inst),
  ST_GeomFromText('POLYGON((23.62 37.93, 23.67 37.93, 23.67 37.97, 23.62 37.97, 23.62 37.93))', 4326),
  0.0)
ORDER BY vessel_id, gettimestamp(inst)
LIMIT 1000;

-- Medium Polygon - Central Saronic Gulf, 1000 ships
SELECT DISTINCT ON (vessel_id)    
  vessel_id,
  getvalue(inst) AS geom,
  gettimestamp(inst) AS datetime
FROM (
  SELECT
    vessel_id,
    unnest(instants(traj)) AS inst
  FROM navigation_data.vessel_trajectories
  WHERE bbox && stbox(
    ST_GeomFromText('POLYGON((23.45 37.85, 23.75 37.85, 23.75 38.05, 23.45 38.05, 23.45 37.85))', 4326))
) sub
WHERE ST_DWithin(
  getvalue(inst),
  ST_GeomFromText('POLYGON((23.45 37.85, 23.75 37.85, 23.75 38.05, 23.45 38.05, 23.45 37.85))', 4326),
  0.0)
ORDER BY vessel_id, gettimestamp(inst)
LIMIT 1000;

-- Large Polygon - Extended Saronic Gulf, 1000 ships
SELECT DISTINCT ON (vessel_id)    
  vessel_id,
  getvalue(inst) AS geom,
  gettimestamp(inst) AS datetime
FROM (
  SELECT
    vessel_id,
    unnest(instants(traj)) AS inst
  FROM navigation_data.vessel_trajectories
  WHERE bbox && stbox(
    ST_GeomFromText('POLYGON((23.25 37.70, 23.85 37.70, 23.85 38.15, 23.25 38.15, 23.25 37.70))', 4326))
) sub
WHERE ST_DWithin(
  getvalue(inst),
  ST_GeomFromText('POLYGON((23.25 37.70, 23.85 37.70, 23.85 38.15, 23.25 38.15, 23.25 37.70))', 4326),
  0.0)
ORDER BY vessel_id, gettimestamp(inst)
LIMIT 1000;
------------------------------------------- Q5 Range Query -------------------------------------------------------------
-- 1 month, small bouding box
SELECT 
  vessel_id,
  getvalue(inst) AS geom,
  gettimestamp(inst) AS datetime
FROM (
  SELECT 
    vessel_id,
    unnest(instants(
      atgeometrytime(
        traj,
        ST_MakeEnvelope(23.675, 37.925, 23.725, 37.975, 4326),
        '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan) ))  AS inst
  FROM navigation_data.vessel_trajectories
  WHERE start_time <= '2018-05-31 23:59:59'
    AND end_time >= '2018-05-01 00:00:00'
    AND bbox && stbox(
      ST_MakeEnvelope(23.675, 37.925, 23.725, 37.975, 4326),
      '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan)) sub
ORDER BY vessel_id, gettimestamp(inst);

-- 3 months, medium bouding box
SELECT 
  vessel_id,
  getvalue(inst) AS geom,
  gettimestamp(inst) AS datetime
FROM (
  SELECT
    vessel_id,
    unnest(instants(
      atgeometrytime(
        traj,
        ST_MakeEnvelope(23.65, 37.9, 23.75, 38.0, 4326) ,
        '[2018-05-01 00:00:00+00, 2018-07-31 23:59:59+00]'::tstzspan) ))  AS inst
  FROM navigation_data.vessel_trajectories
  WHERE start_time <= '2018-07-31 23:59:59'
    AND end_time >= '2018-05-01 00:00:00'
    AND bbox && stbox(
      ST_MakeEnvelope(23.65, 37.9, 23.75, 38.0, 4326) ,
      '[2018-05-01 00:00:00+00, 2018-07-31 23:59:59+00]'::tstzspan)) sub
ORDER BY vessel_id, gettimestamp(inst);

-- 6 months, large bouding box
SELECT 
  vessel_id,
  getvalue(inst) AS geom,
  gettimestamp(inst) AS datetime
FROM (
  SELECT
    vessel_id,
    unnest(instants(
      atgeometrytime(
        traj,
        ST_MakeEnvelope(23.5, 37.8, 24.0, 38.3, 4326) ,
        '[2018-05-01 00:00:00+00, 2018-10-31 23:59:59+00]'::tstzspan) ))  AS inst
  FROM navigation_data.vessel_trajectories
  WHERE start_time <= '2018-10-31 23:59:59'
    AND end_time >= '2018-05-01 00:00:00'
    AND bbox && stbox(
      ST_MakeEnvelope(23.5, 37.8, 24.0, 38.3, 4326) ,
      '[2018-05-01 00:00:00+00, 2018-10-31 23:59:59+00]'::tstzspan)) sub
ORDER BY vessel_id, gettimestamp(inst);
------------------------------------------- Q6 Distance Join -------------------------------------------------------------
-- 1km, 1 month, 1000 ships
WITH candidate_vessels AS (
  SELECT vessel_id
  FROM navigation_data.vessel_trajectories
  LIMIT 1000),
vessel_instants AS (
  SELECT
    vt.vessel_id,
    gettimestamp(inst) AS datetime,
    getvalue(inst) AS geom
  FROM navigation_data.vessel_trajectories vt
  JOIN candidate_vessels cv ON vt.vessel_id = cv.vessel_id
  JOIN LATERAL unnest(instants(vt.traj)) AS inst ON TRUE
  WHERE gettimestamp(inst) >= '2018-05-01 00:00:00+00'
    AND gettimestamp(inst) <= '2018-05-31 23:59:59+00')
SELECT 
  a.vessel_id AS vessel_a,
  b.vessel_id AS vessel_b,
  a.datetime,
  ST_Distance(
    ST_Transform(a.geom, 3857),
    ST_Transform(b.geom, 3857)) AS distance_meters
FROM vessel_instants a
JOIN vessel_instants b
  ON a.datetime = b.datetime
 AND a.vessel_id < b.vessel_id
WHERE ST_DWithin(
  ST_Transform(a.geom, 3857),
  ST_Transform(b.geom, 3857),
  1000.0);

-- 2km, 1 month, 1000 ships
WITH candidate_vessels AS (
  SELECT vessel_id
  FROM navigation_data.vessel_trajectories
  LIMIT 1000),
vessel_instants AS (
  SELECT
    vt.vessel_id,
    gettimestamp(inst) AS datetime,
    getvalue(inst) AS geom
  FROM navigation_data.vessel_trajectories vt
  JOIN candidate_vessels cv ON vt.vessel_id = cv.vessel_id
  JOIN LATERAL unnest(instants(vt.traj)) AS inst ON TRUE
  WHERE gettimestamp(inst) >= '2018-05-01 00:00:00+00'
    AND gettimestamp(inst) <= '2018-05-31 23:59:59+00')
SELECT 
  a.vessel_id AS vessel_a,
  b.vessel_id AS vessel_b,
  a.datetime,
  ST_Distance(
    ST_Transform(a.geom, 3857),
    ST_Transform(b.geom, 3857)) AS distance_meters
FROM vessel_instants a
JOIN vessel_instants b
  ON a.datetime = b.datetime
 AND a.vessel_id < b.vessel_id
WHERE ST_DWithin(
  ST_Transform(a.geom, 3857),
  ST_Transform(b.geom, 3857),
  2000.0);

-- 3km, 1 month, 1000 ships  
WITH candidate_vessels AS (
  SELECT vessel_id
  FROM navigation_data.vessel_trajectories
  LIMIT 1000),
vessel_instants AS (
  SELECT
    vt.vessel_id,
    gettimestamp(inst) AS datetime,
    getvalue(inst) AS geom
  FROM navigation_data.vessel_trajectories vt
  JOIN candidate_vessels cv ON vt.vessel_id = cv.vessel_id
  JOIN LATERAL unnest(instants(vt.traj)) AS inst ON TRUE
  WHERE gettimestamp(inst) >= '2018-05-01 00:00:00+00'
    AND gettimestamp(inst) <= '2018-05-31 23:59:59+00')
SELECT 
  a.vessel_id AS vessel_a,
  b.vessel_id AS vessel_b,
  a.datetime,
  ST_Distance(
    ST_Transform(a.geom, 3857),
    ST_Transform(b.geom, 3857)) AS distance_meters
FROM vessel_instants a
JOIN vessel_instants b
  ON a.datetime = b.datetime
 AND a.vessel_id < b.vessel_id
WHERE ST_DWithin(
  ST_Transform(a.geom, 3857),
  ST_Transform(b.geom, 3857),
  3000.0);
------------------------------------------- Q7 Nearest Neighbor -------------------------------------------------------------
-- 100 ships, 1 month
SELECT DISTINCT ON (v.vessel_id)
  v.vessel_id,
  gettimestamp(inst) AS datetime,
  getvalue(inst) AS geom,
  ST_Distance(
    ST_Transform(getvalue(inst), 3857),
    ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)
  ) AS distance_meters
FROM navigation_data.vessel_trajectories v
CROSS JOIN LATERAL unnest(
  instants(
    atTime(
      v.traj, 
      tstzspan('[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]')))
) AS inst
ORDER BY v.vessel_id, distance_meters
LIMIT 100;

-- 1000 ships, 1 month
SELECT DISTINCT ON (v.vessel_id)
  v.vessel_id,
  gettimestamp(inst) AS datetime,
  getvalue(inst) AS geom,
  ST_Distance(
    ST_Transform(getvalue(inst), 3857),
    ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)
  ) AS distance_meters
FROM navigation_data.vessel_trajectories v
CROSS JOIN LATERAL unnest(
  instants(
    atTime(
      v.traj, 
      tstzspan('[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]')))
) AS inst
ORDER BY v.vessel_id, distance_meters
LIMIT 1000;

-- 3000 ships, 1 month
SELECT DISTINCT ON (v.vessel_id)
  v.vessel_id,
  gettimestamp(inst) AS datetime,
  getvalue(inst) AS geom,
  ST_Distance(
    ST_Transform(getvalue(inst), 3857),
    ST_Transform(ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326), 3857)
  ) AS distance_meters
FROM navigation_data.vessel_trajectories v
CROSS JOIN LATERAL unnest(
  instants(
    atTime(
      v.traj, 
      tstzspan('[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]')))
) AS inst
ORDER BY v.vessel_id, distance_meters
LIMIT 3000;

--using different functions, more expensive
'SELECT DISTINCT ON (vessel_id) vessel_id,
  nearestapproachinstant(traj, ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326)) AS approach_instant,
  gettimestamp(nearestapproachinstant(traj, ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326))) AS datetime,
  getvalue(nearestapproachinstant(traj, ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326))) AS geom,
  nearestapproachdistance(traj, ST_SetSRID(ST_MakePoint(23.637, 37.942), 4326)) AS distance_meters
FROM navigation_data.vessel_trajectories
WHERE start_time <= '2018-05-31 23:59:59'
  AND end_time >= '2018-05-01 00:00:00'
ORDER BY vessel_id, distance_meters
LIMIT 100;'
------------------------------------------- Q8 Clustering -------------------------------------------------------------
--Small Polygon - Piraeus Port Area, 25 meters, 1 month
SELECT cluster_id, ST_Centroid(ST_Collect(geom)) AS cluster_geom
FROM (SELECT 
    ST_ClusterDBSCAN(ST_Transform(geom, 3857), 25, 3) OVER () AS cluster_id,
    geom
  FROM (SELECT getvalue(inst) AS geom
    FROM (SELECT 
        unnest(instants(
          atgeometrytime(
            traj,
            (SELECT geom FROM geographical_data.piraeus_port),
            '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan))) AS inst
      FROM navigation_data.vessel_trajectories
      WHERE start_time <= '2018-05-31 23:59:59'
        AND end_time >= '2018-05-01 00:00:00'
        AND bbox && stbox(
          (SELECT geom FROM geographical_data.piraeus_port),
          '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan)) AS extracted
  ) AS extracted_points
) AS clustered
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id;

-- Medium Polygon - Central Saronic Gulf, 500 meters, 1 month
SELECT cluster_id, ST_Centroid(ST_Collect(geom)) AS cluster_geom
FROM (SELECT 
    ST_ClusterDBSCAN(ST_Transform(geom, 3857), 500, 3) OVER () AS cluster_id,
    geom
  FROM (SELECT getvalue(inst) AS geom
    FROM (SELECT 
        unnest(instants(
          atgeometrytime(
            traj,
            ST_GeomFromText('POLYGON((23.45 37.85, 23.75 37.85, 23.75 38.05, 23.45 38.05, 23.45 37.85))', 4326),
            '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan))) AS inst
      FROM navigation_data.vessel_trajectories
      WHERE start_time <= '2018-05-31 23:59:59'
        AND end_time >= '2018-05-01 00:00:00'
        AND bbox && stbox(ST_GeomFromText('POLYGON((23.45 37.85, 23.75 37.85, 23.75 38.05, 23.45 38.05, 23.45 37.85))', 4326),
          '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan)) AS extracted
  ) AS extracted_points
) AS clustered
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id;

-- Large Polygon - Extended Saronic Gulf, 500 meters, 1 month
SELECT cluster_id, ST_Centroid(ST_Collect(geom)) AS cluster_geom
FROM (SELECT 
    ST_ClusterDBSCAN(ST_Transform(geom, 3857), 25, 3) OVER () AS cluster_id,
    geom
  FROM (SELECT getvalue(inst) AS geom
    FROM (SELECT 
        unnest(instants(
          atgeometrytime(
            traj,
            (SELECT geom FROM geographical_data.territorial_waters),
            '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan))) AS inst
      FROM navigation_data.vessel_trajectories
      WHERE start_time <= '2018-05-31 23:59:59'
        AND end_time >= '2018-05-01 00:00:00'
        AND bbox && stbox(
          (SELECT geom FROM geographical_data.territorial_waters),
          '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan)) AS extracted
  ) AS extracted_points
) AS clustered
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id;
------------------------------------------- Q9 Intersect Join ---------------------------------------------------------
-- 20 areas, 1 month
WITH limited_zones AS (
  SELECT * 
  FROM geographical_data.antenna_coverage
  ORDER BY area_id 
  LIMIT 20),
intersections AS (
  SELECT 
    v.vessel_id,
    gettimestamp(instant) AS datetime,
    z.area_id,
    getvalue(instant) AS geom
  FROM limited_zones z
  JOIN navigation_data.vessel_trajectories v
    ON v.bbox && stbox(z.geom, '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan)
  CROSS JOIN LATERAL (
    SELECT unnest(instants(
      atGeometryTime(
        v.traj,
        z.geom,
        '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan))) AS instant) AS i)
SELECT DISTINCT ON (vessel_id, area_id)
  vessel_id, datetime, area_id, geom
FROM intersections
ORDER BY vessel_id, area_id, datetime;

-- 50 areas, 1 month
WITH limited_zones AS (
  SELECT * 
  FROM geographical_data.antenna_coverage
  ORDER BY area_id 
  LIMIT 50),
intersections AS (
  SELECT 
    v.vessel_id,
    gettimestamp(instant) AS datetime,
    z.area_id,
    getvalue(instant) AS geom
  FROM limited_zones z
  JOIN navigation_data.vessel_trajectories v
    ON v.bbox && stbox(z.geom, '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan)
  CROSS JOIN LATERAL (
    SELECT unnest(instants(
      atGeometryTime(
        v.traj,
        z.geom,
        '[2018-05-01 00:00:00+00, 2018-05-31 23:59:59+00]'::tstzspan))) AS instant) AS i)
SELECT DISTINCT ON (vessel_id, area_id)
  vessel_id, datetime, area_id, geom
FROM intersections
ORDER BY vessel_id, area_id, datetime;

-- all areas, 1 month
SELECT DISTINCT ON (vt.vessel_id, z.area_id)
  vt.vessel_id,
  gettimestamp(inst) AS datetime,
  z.area_id,
  getvalue(inst) AS geom
FROM navigation_data.vessel_trajectories vt
JOIN geographical_data.antenna_coverage z 
  ON vt.bbox && stbox(z.geom)
CROSS JOIN LATERAL unnest(
  instants(
    atGeometryTime(
      vt.traj,
      z.geom,
      '[2018-05-01 00:00:00+00,2018-05-31 23:59:59+00]'::tstzspan))) AS inst
ORDER BY vt.vessel_id, z.area_id, gettimestamp(inst);
------------------------------------------- Q10 Weather Related -------------------------------------------------------
-- Ships sailing during stormy winds
SELECT 
  v.vessel_id,
  gettimestamp(inst) AS datetime,
  speed(inst) AS speed,
  getvalue(inst) AS geom
FROM weather_data.noaa_weather w
JOIN meta_data.trajectory_synopses_mobilitydb s
  ON TRUE  
JOIN navigation_data.vessel_trajectories v
  ON v.vessel_id = s.vessel_id
 AND TO_TIMESTAMP(s.t) BETWEEN v.start_time - INTERVAL '30 minutes'
                          AND v.end_time + INTERVAL '30 minutes'
 AND v.bbox && stbox(ST_Buffer(w.geom, 7000))
CROSS JOIN LATERAL unnest(
  instants(
    atGeometry(
      v.traj,
      ST_Buffer(w.geom, 7000)))) AS inst
WHERE w.WSPD > 20;

--  Wind gusts with sudden speed drops in a year
WITH insts AS (
  SELECT 
    v.vessel_id,
    gettimestamp(inst) AS datetime,
    inst,
    LAG(inst) OVER (PARTITION BY v.vessel_id ORDER BY gettimestamp(inst)) AS prev_inst,
    w.GUST
  FROM weather_data.noaa_weather w
  JOIN meta_data.trajectory_synopses_mobilitydb s ON TRUE  
  JOIN navigation_data.vessel_trajectories v
    ON v.vessel_id = s.vessel_id
   AND TO_TIMESTAMP(s.t) BETWEEN v.start_time - INTERVAL '30 minutes'
                            AND v.end_time + INTERVAL '30 minutes'
   AND v.bbox && stbox(ST_Buffer(w.geom, 7000))
  CROSS JOIN LATERAL unnest(
    instants(
      atGeometry(
        v.traj,
        ST_Buffer(w.geom, 7000)))
  ) AS inst
  WHERE w.GUST > 30
 AND w.timestamp BETWEEN '2018-05-01 00:00:00' AND '2018-10-31 23:59:59')
SELECT 
  vessel_id,
  datetime,
  ST_Distance(getvalue(inst), getvalue(prev_inst)) 
    / EXTRACT(EPOCH FROM datetime - gettimestamp(prev_inst)) AS approx_speed,
  GUST
FROM insts
WHERE prev_inst IS NOT NULL
  AND ST_Distance(getvalue(inst), getvalue(prev_inst)) 
    / EXTRACT(EPOCH FROM datetime - gettimestamp(prev_inst)) < 5
ORDER BY datetime;
------------------------------------------- Q11 Semantics -------------------------------------------------------------
-- STOP_START annotations
SELECT 
  m.vessel_id,
  m.datetime AS annotated_time,
  valueAtTimestamp(m.traj, m.datetime) AS geom,
  valueAtTimestamp(m.speed, m.datetime) AS speed,
  valueAtTimestamp(m.heading, m.datetime) AS heading,
  valueAtTimestamp(m.course, m.datetime) AS course
FROM meta_data.trajectory_synopses_mobilitydb m
WHERE m.annotations ILIKE '%STOP_START%'
  AND m.traj IS NOT NULL;

-- High-Density Routes based on STOP_START annotations in a day
WITH time_periods AS (
  SELECT 
    vessel_id,
    datetime AS annotated_time,
    span(
      datetime - INTERVAL '30 minutes',
      datetime + INTERVAL '30 minutes',
      true, true
    ) AS time_window
  FROM meta_data.trajectory_synopses_mobilitydb 
  WHERE annotations ILIKE '%STOP_START%'
  AND datetime BETWEEN '2018-05-01 00:00:00' AND '2018-5-01 23:59:59'),
vessel_trajectory_slices AS (
  SELECT 
    tp.vessel_id,
    tp.annotated_time,
    atTime(vt.traj, tp.time_window) AS sliced_trajectory
  FROM time_periods tp
  JOIN navigation_data.vessel_trajectories vt
    ON tp.vessel_id = vt.vessel_id
  WHERE vt.traj IS NOT NULL
    AND temporal_overlaps(vt.traj, tp.time_window))
SELECT 
  vessel_id,
  annotated_time,
  trajectory(sliced_trajectory) AS trail_geom
FROM vessel_trajectory_slices
WHERE sliced_trajectory IS NOT NULL;


















