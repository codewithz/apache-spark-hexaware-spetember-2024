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