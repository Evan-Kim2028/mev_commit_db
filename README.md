# db

# Clickhouse Setup:
With a new clickhouse instance, need to make a new database in the client. First enter the docker environment:
```docker exec -it mev_commit_db-db-1 clickhouse-client```

Then create new database:
```CREATE DATABASE mev_commit_testnet```

Query from database table:
```SELECT * FROM mev_commit_testnet.newl1block LIMIT 50;```

Count all rows from every table and show table size (in mb):
```SELECT
    table AS table_name,
    sum(rows) AS row_count,
    round(sum(data_uncompressed_bytes) / 1048576, 3) AS uncompressed_size_mb,
    round(sum(data_compressed_bytes) / 1048576, 3) AS compressed_size_mb
FROM
    system.parts
WHERE
    database = 'mev_commit_testnet'
    AND active = 1
GROUP BY
    table;```


