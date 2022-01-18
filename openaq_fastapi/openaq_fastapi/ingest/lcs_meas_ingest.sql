do $$
DECLARE
reject_count int;
insert_count int;
match_count int;
message text;
BEGIN

DELETE FROM meas WHERE ingest_id IS NULL OR datetime is NULL or value IS NULL;
DELETE FROM meas WHERE datetime < '2018-01-01'::timestamptz or datetime>now();

RAISE NOTICE '% records are in the meas table', (SELECT COUNT(1) FROM meas);

WITH m AS (
UPDATE meas
    SET
    sensors_id=s.sensors_id
    FROM sensors s
    WHERE
    s.source_id=ingest_id
    RETURNING 1)
SELECT COUNT(1) INTO match_count
FROM m;

RAISE NOTICE '% records were matched using the source & ingest ids', match_count;

WITH r AS (
INSERT INTO rejects (tbl,r, reason) SELECT
    'meas',
    to_jsonb(meas),
    'SENSOR_MISSING'
FROM meas
WHERE sensors_id IS NULL
RETURNING 1)
SELECT COUNT(1) INTO reject_count
FROM r;

RAISE NOTICE '% records were rejected due to missing sensor', reject_count;


DELETE FROM meas WHERE sensors_id IS NULL;

-- --Some fake data to make it easier to test this section
-- TRUNCATE meas;
-- INSERT INTO meas (ingest_id, sensors_id, value, datetime)
-- SELECT 'fake-ingest'
-- , (SELECT sensors_id FROM sensors ORDER BY random() LIMIT 1)
-- , -99
-- , generate_series(now() - '3day'::interval, current_date, '1hour'::interval);


WITH m AS (
INSERT INTO measurements (
    sensors_id,
    datetime,
    value,
    lon,
    lat
) SELECT
    DISTINCT
    sensors_id,
    datetime,
    value,
    lon,
    lat
FROM
    meas
WHERE
    sensors_id IS NOT NULL
ON CONFLICT DO NOTHING
RETURNING 1)
SELECT COUNT(1)
FROM m INTO insert_count;

RAISE NOTICE '% records were inserted', insert_count;

IF reject_count > 0 THEN
   RAISE NOTICE 'explain here';
END IF;

END $$;


DO $$
BEGIN
  -- Update the export queue/logs to export these records
  -- wrap it in a block just in case the database does not have this module installed
  -- we subtract the second because the data is assumed to be time ending
  INSERT INTO open_data_export_logs (sensor_nodes_id, day, records, measurands, modified_on)
  SELECT sn.sensor_nodes_id
  , ((m.datetime - '1sec'::interval) AT TIME ZONE (sn.metadata->>'timezone')::text)::date as day
  , COUNT(1)
  , COUNT(DISTINCT p.measurands_id)
  , MAX(now())
  FROM meas m
  JOIN sensors s ON (m.sensors_id = s.sensors_id)
  JOIN measurands p ON (s.measurands_id = p.measurands_id)
  JOIN sensor_systems ss ON (s.sensor_systems_id = ss.sensor_systems_id)
  JOIN sensor_nodes sn ON (ss.sensor_nodes_id = sn.sensor_nodes_id)
  GROUP BY sn.sensor_nodes_id
  , ((m.datetime - '1sec'::interval) AT TIME ZONE (sn.metadata->>'timezone')::text)::date
  ON CONFLICT (sensor_nodes_id, day) DO UPDATE
  SET records = EXCLUDED.records
  , measurands = EXCLUDED.measurands
  , modified_on = EXCLUDED.modified_on;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Failed to export to logs: %', SQLERRM
    USING HINT = 'Make sure that the open data module is installed';
END
$$;
