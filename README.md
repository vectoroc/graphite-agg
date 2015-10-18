# Graphite metrics aggregator

Stupid simple script to aggregate (currently sum, min, max, avg) metrics from several servers.
May be useful if you do not want to bother with graphite directly

## Usage 

    python graphite-agg.py --port XXXX | nc -q0 graphite.example.com 2003


## Requirements 

See requirements.txt