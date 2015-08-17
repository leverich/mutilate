# m2i

Publishes stats from memcached to influxdb.

Currently, the only metric published is _requests per second_.

## Usage

```
$ ./m2i.py --help
usage: m2i.py [-h] [--memcached-host [MEMCACHED_HOST]]
              [--memcached-port [MEMCACHED_PORT]]
              [--memcached-timeout-seconds [MEMCACHED_TIMEOUT_SECONDS]]
              [--influxdb-host [INFLUXDB_HOST]]
              [--influxdb-port [INFLUXDB_PORT]] --influxdb-db-name
              INFLUXDB_DB_NAME [--influxdb-user INFLUXDB_USER]
              [--influxdb-password INFLUXDB_PASSWORD]
              [--stats-interval-seconds [STATS_INTERVAL_SECONDS]]
```

## Example

```
$ ./m2i.py --influxdb-db-name test

m2i.py

memcached_host            : localhost
memcached_port            : 11211
memcached_timeout_seconds : 5
influxdb_host             : localhost
influxdb_port             : 8086
stats_interval_seconds    : 1

Scheduling sample collection for every [1] seconds
Requesting stats from memcached at [localhost:11211]
Skipping requests-per-second report for this sample
Requesting stats from memcached at [localhost:11211]
Reporting [15282] requests per second
Posting metrics to influxdb
Requesting stats from memcached at [localhost:11211]
Reporting [15992] requests per second
Posting metrics to influxdb
Requesting stats from memcached at [localhost:11211]
Reporting [15736] requests per second
Posting metrics to influxdb
```

## Querying data from InfluxDB

Simplest example:

```sql
SELECT value from memcached_rps;
```

