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