SELECT 'Yellow'                   AS TaxiType

      , lpep_pickup_datetime      AS PickupTime
      , lpep_dropoff_datetime     AS DropTime
      , PULocationID              AS PickupLocationId
      , DOLocationID              AS DropLocationId
FROM YellowTaxis

UNION ALL

SELECT 'Green'                    AS TaxiType

      , lpep_pickup_datetime      AS PickupTime
      , lpep_dropoff_datetime     AS DropTime
      , PULocationID              AS PickupLocationId
      , DOLocationID              AS DropLocationId
FROM GreenTaxis

-------------------------------------------------------------------

SELECT Borough, TaxiType, COUNT(*) AS TotalTrips

FROM TaxiZones

LEFT JOIN
(

    SELECT 'Yellow' AS TaxiType, PULocationID FROM YellowTaxis

    UNION ALL

    SELECT 'Green' AS TaxiType, PULocationID FROM GreenTaxis

) AllTaxis

ON AllTaxis.PULocationID = TaxiZones.LocationID

GROUP BY Borough, TaxiType

ORDER BY Borough, TaxiType
------------------------------------------------------------------------------

    SELECT DISTINCT tz.*

    FROM TaxiZones tz

        LEFT JOIN YellowTaxis yt ON yt.PickupLocationId = tz.PickupLocationId

        WHERE yt.PickupLocationId IS NULL


--        --------- List of all the drivers ----------

    (
        SELECT Name
        FROM Cabs
        WHERE LicenseType = 'OWNER MUST DRIVE'
    )

    UNION ALL

    (
        SELECT Name
        FROM Drivers
    )


--    ----------List of Unique Drivers --------------

    (
        SELECT Name
        FROM Cabs
        WHERE LicenseType = 'OWNER MUST DRIVE'
    )

    UNION

    (
        SELECT Name
        FROM Drivers
    )

--    ------ Create a List of Drivers who are driving the cabs

    (
        SELECT Name
        FROM Cabs
        WHERE LicenseType = 'OWNER MUST DRIVE'
    )

    INTERSECT

    (
        SELECT Name
        FROM Drivers
    )


--    ---List of Drivers who are driving the cab but not registered

  (
        SELECT Name
        FROM Cabs
        WHERE LicenseType = 'OWNER MUST DRIVE'
    )

    EXCEPT

    (
        SELECT Name
        FROM Drivers
    )

--    Windows Operations

            SELECT tz.Borough
                     , COUNT(*)     AS RideCount

                FROM TaxiZones tz
                    INNER JOIN YellowTaxis yt ON yt.PULocationID = tz.LocationId

                GROUP BY tz.Borough

                +-------------+---------+
                |      Borough|RideCount|
                +-------------+---------+
                |        Bronx|     4511|
                |     Brooklyn|    28089|
                |          EWR|     1157|
                |    Manhattan|  3250695|
                |       Queens|   333922|
                |Staten Island|      303|
                |      Unknown|    56735|
                +-------------+---------+


--             TaxiRides

                    SELECT *
                         , SUM (RideCount)   OVER ()   AS TotalRideCount

                    FROM TaxiRides

--                    Share of each borough

                SELECT *
                  , ROUND( (RideCount * 100) / TotalRideCount, 2)   AS RidesSharePercent

            FROM TaxiRidesWindow
            ORDER BY Borough

--            //        Find the share of each zone in terms of rides within their Borough

--              //        Zone is a part of Borough

--              //        1. Get rides for each zone
                      SELECT tz.Borough
                         , tz.Zone
                         , COUNT(*)     AS RideCount

                    FROM TaxiZones tz
                        INNER JOIN YellowTaxis yt ON yt.PULocationId = tz.LocationId

                    GROUP BY tz.Borough
                           , tz.Zone

                           +-------+--------------------------------+---------+
                           |Borough|Zone                            |RideCount|
                           +-------+--------------------------------+---------+
                           |Bronx  |Allerton/Pelham Gardens         |51       |
                           |Bronx  |Bedford Park                    |92       |
                           |Bronx  |Belmont                         |59       |
                           |Bronx  |Bronx Park                      |22       |
                           |Bronx  |Bronxdale                       |48       |
                           |Bronx  |City Island                     |11       |
                           |Bronx  |Claremont/Bathgate              |98       |
                           |Bronx  |Co-Op City                      |200      |
                           |Bronx  |Country Club                    |7        |
                           |Bronx  |Crotona Park                    |2        |
                           |Bronx  |Crotona Park East               |45       |
                           |Bronx  |East Concourse/Concourse Village|180      |
                           |Bronx  |East Tremont                    |113      |
                           |Bronx  |Eastchester                     |73       |
                           |Bronx  |Fordham South                   |52       |
                           |Bronx  |Highbridge                      |158      |
                           |Bronx  |Hunts Point                     |67       |
                           |Bronx  |Kingsbridge Heights             |116      |
                           |Bronx  |Longwood                        |41       |
                           |Bronx  |Melrose South                   |173      |
                           +-------+--------------------------------+---------+
                           only showing top 20 rows


--              //        2. Create a Window over entire table partitioned by Borough

                      SELECT *
                      , SUM (RideCount)     OVER (PARTITION BY Borough)    AS TotalRideCountByBorough

                         FROM TaxiRides

                         +-------+--------------------------------+---------+-----------------------+
                         |Borough|Zone                            |RideCount|TotalRideCountByBorough|
                         +-------+--------------------------------+---------+-----------------------+
                         |Bronx  |Allerton/Pelham Gardens         |51       |4511                   |
                         |Bronx  |Bedford Park                    |92       |4511                   |
                         |Bronx  |Belmont                         |59       |4511                   |
                         |Bronx  |Bronx Park                      |22       |4511                   |
                         |Bronx  |Bronxdale                       |48       |4511                   |
                         |Bronx  |City Island                     |11       |4511                   |
                         |Bronx  |Claremont/Bathgate              |98       |4511                   |
                         |Bronx  |Co-Op City                      |200      |4511                   |
                         |Bronx  |Country Club                    |7        |4511                   |
                         |Bronx  |Crotona Park                    |2        |4511                   |
                         |Bronx  |Crotona Park East               |45       |4511                   |
                         |Bronx  |East Concourse/Concourse Village|180      |4511                   |
                         |Bronx  |East Tremont                    |113      |4511                   |
                         |Bronx  |Eastchester                     |73       |4511                   |
                         |Bronx  |Fordham South                   |52       |4511                   |
                         |Bronx  |Highbridge                      |158      |4511                   |
                         |Bronx  |Hunts Point                     |67       |4511                   |
                         |Bronx  |Kingsbridge Heights             |116      |4511                   |
                         |Bronx  |Longwood                        |41       |4511                   |
                         |Bronx  |Melrose South                   |173      |4511                   |
                         +-------+--------------------------------+---------+-----------------------+
--              //        3. Add Total RIdes (across all zones in a borough)
--              //        4. Calculate Pct

 SELECT *
          , ROUND( (RideCount * 100) / TotalRideCountByBorough, 2)   AS RidesSharePercentInBorough

    FROM TaxiRidesWindow
    ORDER BY Borough, Zone

    +-------+--------------------------------+---------+-----------------------+--------------------------+
    |Borough|Zone                            |RideCount|TotalRideCountByBorough|RidesSharePercentInBorough|
    +-------+--------------------------------+---------+-----------------------+--------------------------+
    |Bronx  |Allerton/Pelham Gardens         |51       |4511                   |1.13                      |
    |Bronx  |Bedford Park                    |92       |4511                   |2.04                      |
    |Bronx  |Belmont                         |59       |4511                   |1.31                      |
    |Bronx  |Bronx Park                      |22       |4511                   |0.49                      |
    |Bronx  |Bronxdale                       |48       |4511                   |1.06                      |
    |Bronx  |City Island                     |11       |4511                   |0.24                      |
    |Bronx  |Claremont/Bathgate              |98       |4511                   |2.17                      |
    |Bronx  |Co-Op City                      |200      |4511                   |4.43                      |
    |Bronx  |Country Club                    |7        |4511                   |0.16                      |
    |Bronx  |Crotona Park                    |2        |4511                   |0.04                      |
    |Bronx  |Crotona Park East               |45       |4511                   |1.0                       |
    |Bronx  |East Concourse/Concourse Village|180      |4511                   |3.99                      |
    |Bronx  |East Tremont                    |113      |4511                   |2.5                       |
    |Bronx  |Eastchester                     |73       |4511                   |1.62                      |
    |Bronx  |Fordham South                   |52       |4511                   |1.15                      |
    |Bronx  |Highbridge                      |158      |4511                   |3.5                       |
    |Bronx  |Hunts Point                     |67       |4511                   |1.49                      |
    |Bronx  |Kingsbridge Heights             |116      |4511                   |2.57                      |
    |Bronx  |Longwood                        |41       |4511                   |0.91                      |
    |Bronx  |Melrose South                   |173      |4511                   |3.84                      |
    +-------+--------------------------------+---------+-----------------------+------------------------



    CREATE TABLE TaxisDB.YellowTaxis
    (
        VendorId                INT               COMMENT 'Vendor providing the ride',

        PickupTime              TIMESTAMP,
        DropTime                TIMESTAMP,

        PickupLocationId        INT               NOT NULL,
        DropLocationId          INT,

        PassengerCount          DOUBLE,
        TripDistance            DOUBLE,

        RateCodeId              DOUBLE,
        StoreAndFwdFlag         STRING,
        PaymentType             INT,

        FareAmount              DOUBLE,
        Extra                   DOUBLE,
        MtaTax                  DOUBLE,
        TipAmount               DOUBLE,
        TollsAmount             DOUBLE,
        ImprovementSurcharge    DOUBLE,
        TotalAmount             DOUBLE,
        CongestionSurcharge     DOUBLE,
        AirportFee              DOUBLE
    )

    USING DELTA                  -- default is Parquet

    LOCATION "C:/SparkCourse/DataFiles/Output/YellowTaxis.delta/"

    PARTITIONED BY (VendorId)    -- optional

    COMMENT 'This table stores ride information for Yellow Taxis'

--    -- INSERT QUERY

INSERT INTO TaxisDB.YellowTaxis

-- (VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, PassengerCount, TripDistance, RateCodeId, StoreAndFwdFlag, PaymentType, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge, TotalAmount, CongestionSurcharge, AirportFee)

VALUES (3, '2022-12-01T00:00:00.000Z', '2022-12-01T00:15:34.000Z', 170, 140, 1.0, 2.9, 1.0, '1', 1, 13.0, 0.5, 0.5, 1.0, 0.0, 0.3, 15.3, 0.0, 0.0)


-- Chgeck files holding the data

SELECT INPUT_FILE_NAME()

     , VendorId
     , PickupLocationId
     , PassengerCount

FROM TaxisDB.YellowTaxis

WHERE VendorId = 3

-- UPDATE

UPDATE TaxisDB.YellowTaxis

SET PassengerCount = 2

WHERE VendorId = 3
    AND PickupLocationId = 249


--    DELETE

DELETE FROM TaxisDB.YellowTaxis

WHERE VendorId = 3
    AND PickupLocationId = 151

----    yellowTaxiChangesDF = (
--                                spark
--                                  .read
--                                  .option("header", "true")
--                                  .schema(yellowTaxiSchema)
--                                  .csv("C:\SparkCourse\DataFiles\Raw\YellowTaxis_changes.csv")
--                            )
--
--      yellowTaxiChangesDF.show()

--yellowTaxiChangesDF.createOrReplaceTempView("YellowTaxiChanges")


--MERGE

MERGE INTO TaxisDB.YellowTaxis tgt

    USING YellowTaxiChanges    src

        ON    tgt.VendorId          =  src.VendorId
          AND tgt.PickupLocationId  =  src.PickupLocationId

-- Update row if join conditions match
WHEN MATCHED

      THEN
          UPDATE SET    tgt.PaymentType = src.PaymentType

                                                      -- Use 'UPDATE SET *' to update all columns

-- Insert row if row is not present in target table
WHEN NOT MATCHED
      AND PickupTime >= '2022-03-01'

      THEN
          INSERT (VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, PassengerCount, TripDistance,
                  RateCodeId, StoreAndFwdFlag, PaymentType, FareAmount, Extra, MtaTax, TipAmount, TollsAmount,
                  ImprovementSurcharge, TotalAmount, CongestionSurcharge, AirportFee)

          VALUES (VendorId, PickupTime, DropTime, PickupLocationId, DropLocationId, PassengerCount, TripDistance,
                  RateCodeId, StoreAndFwdFlag, PaymentType, FareAmount, Extra, MtaTax, TipAmount, TollsAmount,
                  ImprovementSurcharge, TotalAmount, CongestionSurcharge, AirportFee)