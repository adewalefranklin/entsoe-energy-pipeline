-- create staging layer



 CREATE OR REPLACE STAGE entsoe\_stage

 URL = 's3://day-ahead-prices-adewale-franklin/'

 STORAGE\_INTEGRATION = s3\_entsoe\_init

 FILE\_FORMAT = json\_file\_format;



 LIST @entsoe\_stage;



--create raw JSON table

CREATE OR REPLACE TABLE entsoe\_raw (

  load\_time TIMESTAMP\_NTZ DEFAULT CURRENT\_TIMESTAMP(),

  filename STRING,

  payload VARIANT

);



COPY INTO entsoe\_raw (filename, payload)

FROM (

  SELECT METADATA$FILENAME, $1

  FROM @entsoe\_stage

)

PATTERN = '.\*entsoe/day\_ahead\_prices/.\*\\\\.json'

FILE\_FORMAT = (FORMAT\_NAME = json\_file\_format)

ON\_ERROR = 'CONTINUE';



--create snowpipe for auto ingestion of new file (Snowpipe AUTO\_INGEST requires S3 event notification)



CREATE OR REPLACE PIPE entsoe\_raw\_pipe

  AUTO\_INGEST = TRUE

AS

COPY INTO entsoe\_raw (filename, payload)

FROM (

  SELECT METADATA$FILENAME, $1

  FROM @entsoe\_stage

)

PATTERN = '.\*entsoe/day\_ahead\_prices/.\*\\\\.json'

FILE\_FORMAT = (FORMAT\_NAME = json\_file\_format)

ON\_ERROR = 'CONTINUE';





SHOW PIPES LIKE 'ENTSOE\_RAW\_PIPE';



--create fact table



CREATE OR REPLACE TABLE fact\_day\_ahead\_prices (

  country\_code STRING,

  delivery\_date DATE,

  position INT,

  price\_eur\_mwh FLOAT,

  filename STRING,

  loaded\_at TIMESTAMP\_NTZ

);



--optimized the fact table to enhance analytical performance



ALTER TABLE fact\_day\_ahead\_prices CLUSTER BY (delivery\_date, country\_code);



--create incremental table loading (Backfill MERGE first (loads existing raw data)



MERGE INTO fact\_day\_ahead\_prices t

USING (

  SELECT

    REGEXP\_SUBSTR(filename, '/(\[A-Z]{2}(\_\[A-Z]{2})?)/', 1, 1, 'e', 1) AS country\_code,

    payload:"date"::date                                              AS delivery\_date,

    f.value:"position"::int                                           AS position,

    f.value:"price"::float                                            AS price\_eur\_mwh,

    filename,

    CURRENT\_TIMESTAMP()                                               AS loaded\_at

  FROM entsoe\_raw,

  LATERAL FLATTEN(input => payload:"points") f

  WHERE REGEXP\_SUBSTR(filename, '/(\[A-Z]{2}(\_\[A-Z]{2})?)/', 1, 1, 'e', 1) IS NOT NULL

) s

ON  t.country\_code  = s.country\_code

AND t.delivery\_date = s.delivery\_date

AND t.position      = s.position

WHEN MATCHED THEN UPDATE SET

  t.price\_eur\_mwh = s.price\_eur\_mwh,

  t.filename      = s.filename,

  t.loaded\_at     = s.loaded\_at

WHEN NOT MATCHED THEN INSERT (

  country\_code, delivery\_date, position, price\_eur\_mwh, filename, loaded\_at

) VALUES (

  s.country\_code, s.delivery\_date, s.position, s.price\_eur\_mwh, s.filename, s.loaded\_at

);



--tracking new inserts into RAW (created STREAM to tracks only new inserts going forward)



CREATE OR REPLACE STREAM entsoe\_raw\_stream

ON TABLE entsoe\_raw

APPEND\_ONLY = TRUE;



-- task that merges only new rows whenever new data arrives



CREATE OR REPLACE TASK task\_merge\_day\_ahead\_prices

  WAREHOUSE = COMPUTE\_WH

  SCHEDULE = '5 MINUTE'

AS

MERGE INTO fact\_day\_ahead\_prices t

USING (

  SELECT

    REGEXP\_SUBSTR(filename, '/(\[A-Z]{2}(\_\[A-Z]{2})?)/', 1, 1, 'e', 1) AS country\_code,

    payload:"date"::date                                              AS delivery\_date,

    f.value:"position"::int                                           AS position,

    f.value:"price"::float                                            AS price\_eur\_mwh,

    filename,

    CURRENT\_TIMESTAMP()                                               AS loaded\_at

  FROM entsoe\_raw\_stream,

  LATERAL FLATTEN(input => payload:"points") f

  WHERE REGEXP\_SUBSTR(filename, '/(\[A-Z]{2}(\_\[A-Z]{2})?)/', 1, 1, 'e', 1) IS NOT NULL

) s

ON  t.country\_code  = s.country\_code

AND t.delivery\_date = s.delivery\_date

AND t.position      = s.position

WHEN MATCHED THEN UPDATE SET

  t.price\_eur\_mwh = s.price\_eur\_mwh,

  t.filename      = s.filename,

  t.loaded\_at     = s.loaded\_at

WHEN NOT MATCHED THEN INSERT (

  country\_code, delivery\_date, position, price\_eur\_mwh, filename, loaded\_at

) VALUES (

  s.country\_code, s.delivery\_date, s.position, s.price\_eur\_mwh, s.filename, s.loaded\_at

);



ALTER TASK task\_merge\_day\_ahead\_prices RESUME;



CREATE OR REPLACE VIEW vw\_day\_ahead\_prices AS

SELECT

  country\_code,

  delivery\_date,

  position,

  DATEADD(hour, position - 1, TO\_TIMESTAMP\_NTZ(delivery\_date)) AS delivery\_datetime,

  price\_eur\_mwh

FROM fact\_day\_ahead\_prices;



