view: correlation_table_pdt {
    derived_table: {
      sql: (WITH Sales AS (
            SELECT DISTINCT SalesOrders.MaterialNumber_MATNR AS Product,
              SalesOrders.RequestedDeliveryDate_VDATU AS date,
              SalesOrders.CumulativeOrderQuantity_KWMENG AS SalesOrderQuantity,
              Customers.City_ORT01 AS Location,
              Customers.PostalCode_PSTLZ AS PostalCode
            FROM
              `@{GCP_PROJECT}.@{REPORTING_DATASET}.SalesOrders` AS SalesOrders
            LEFT JOIN
              `@{GCP_PROJECT}.@{REPORTING_DATASET}.CustomersMD` AS Customers
              ON
                SalesOrders.Client_MANDT = Customers.Client_MANDT
                AND SalesOrders.ShipToPartyItem_KUNNR = Customers.CustomerNumber_KUNNR
            WHERE
              SalesOrders.Client_MANDT = "@{CLIENT}"
              AND EXTRACT(YEAR
              FROM
                SalesOrders.RequestedDeliveryDate_VDATU) >= (
              SELECT
                EXTRACT(YEAR
                FROM
                  CURRENT_DATE()) - 3)
          )

        SELECT DISTINCT Sales.Product,
        Sales.Location,
        CORR(Sales.SalesOrderQuantity,
        (Weather.MaxTemp + Weather.MinTemp) / 2) OVER(PARTITION BY Sales.product, Sales.Location) AS CorrValue
        FROM
        Sales
        LEFT JOIN
        `@{GCP_PROJECT}.@{K9_REPORTING_DATASET}.Weather` AS Weather
        ON
        Sales.PostalCode = Weather.PostCode
        AND Sales.date = Weather.WeekStartDate)
        ;;
      interval_trigger: "730 hour"
    }
}
