## Benchmark querying IP geodata

Download the MaxMind GeoLite2 data:

[MaxMind GeoLite2 databases](https://dev.maxmind.com/geoip/geoip2/geolite2/)

Setup a Virtualenv and run:

`pip install -r requirements.txt`

## Create a SQLite DB

    python index_sql.py

## Create a Redis DB

	python index_redis.py

This generates a `migration.txt` that contains the Redis protocol data that defines the three indexes:

- cidr:{{ `cidr` }}
- cidr:index
- geoid:{{ `geonameid` }}

## Run benchmarks

	python benchmark.py

This will try to associate 2,000 random IPv4 addresses but will most likely find 1,200 - 1,700. These benchmarks can be found in the generated `benchmarks.csv`.

