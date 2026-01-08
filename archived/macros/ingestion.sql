{# 
  Macro: generate_parquet_ingestion
  
  Purpose:
    Generates a UNION ALL SQL query that reads parquet files from HTTP URLs for NYC taxi trip data.
    This macro handles multiple months of data by creating UNION ALL statements for each month's parquet file.
    
  Why this approach:
    - NYC TLC provides data in monthly parquet files (one file per taxi type per month)
    - When processing a date range spanning multiple months, we need to read multiple files
    - UNION ALL efficiently combines data from multiple sources without deduplication
    - Adding taxi_type column ensures we can track which taxi type each record represents
    
  Parameters:
    - taxi_type: String, either "yellow" or "green" - determines which taxi type files to read
    - months: List of [year, month] pairs, e.g., [[2022, 1], [2022, 2]] for Jan and Feb 2022
    
  Returns:
    SQL query string with UNION ALL statements reading from HTTP parquet URLs
    
  Example output:
    SELECT *, 'yellow' AS taxi_type
    FROM read_parquet('https://.../yellow_tripdata_2022-01.parquet')
    UNION ALL
    SELECT *, 'yellow' AS taxi_type
    FROM read_parquet('https://.../yellow_tripdata_2022-02.parquet')
#}
{% macro generate_parquet_ingestion(taxi_type, months) -%}
  {% if months | length > 0 %}
    {# 
      First month: SELECT without UNION ALL
      - months[0][0] is the year (e.g., 2022)
      - months[0][1] is the month (e.g., 1 for January)
      - "%02d" format ensures month is always 2 digits (01, 02, ..., 12)
      - This matches the file naming convention: yellow_tripdata_2022-01.parquet
    #}
    SELECT
      *,
      '{{ taxi_type }}' AS taxi_type,
    FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ months[0][0] }}-{{ "%02d" | format(months[0][1]) }}.parquet')
    
    {% if months | length > 1 %}
      {# 
        Additional months: Add UNION ALL for each subsequent month
        - Loop through remaining months (months[1:] means skip first, process rest)
        - Each iteration adds another UNION ALL SELECT statement
        - This builds a query that reads from all required monthly files
      #}
      {% for month_item in months[1:] %}
        UNION ALL
        SELECT
          *,
          '{{ taxi_type }}' AS taxi_type,
        FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/{{ taxi_type }}_tripdata_{{ month_item[0] }}-{{ "%02d" | format(month_item[1]) }}.parquet')
      {% endfor %}
    {% endif %}
  {% else %}
    {# 
      Edge case: No months to process
      - Returns empty result set with correct schema
      - WHERE 1=0 ensures no rows are returned
      - This prevents errors when date range calculation results in empty month list
      - Schema matches expected NYC taxi trip data structure
    #}
    SELECT
      NULL AS vendorid,
      NULL AS pickup_time,
      NULL AS dropoff_time,
      NULL AS passenger_count,
      NULL AS trip_distance,
      NULL AS ratecodeid,
      NULL AS store_and_fwd_flag,
      NULL AS pulocationid,
      NULL AS dolocationid,
      NULL AS payment_type,
      NULL AS fare_amount,
      NULL AS extra,
      NULL AS mta_tax,
      NULL AS tip_amount,
      NULL AS tolls_amount,
      NULL AS improvement_surcharge,
      NULL AS total_amount,
      NULL AS congestion_surcharge,
      NULL AS airport_fee,
      '{{ taxi_type }}' AS taxi_type,
    WHERE 1=0
  {% endif %}
{%- endmacro %}

