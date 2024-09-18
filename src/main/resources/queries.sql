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