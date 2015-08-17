#!/usr/bin/env python

import argparse
import getpass
import json
import requests
import signal
import sys
import telnetlib
import threading

# Parse argument list
parser = argparse.ArgumentParser()
parser.add_argument("--memcached-host", dest="memcached_host", type=str, nargs="?", default="localhost")
parser.add_argument("--memcached-port", dest="memcached_port", type=int, nargs="?", default=11211)
parser.add_argument("--memcached-timeout-seconds", dest="memcached_timeout_seconds", type=int, nargs="?", default=5)
parser.add_argument("--influxdb-host", dest="influxdb_host", type=str, nargs="?", default="localhost")
parser.add_argument("--influxdb-port", dest="influxdb_port", type=int, nargs="?", default=8086)
parser.add_argument("--influxdb-db-name", dest="influxdb_db_name", required=True, type=str)
parser.add_argument("--influxdb-user", dest="influxdb_user", default='root', type=str)
parser.add_argument("--influxdb-password", dest="influxdb_password", default='root', type=str)
parser.add_argument("--stats-interval-seconds", dest="stats_interval_seconds", nargs="?", default=1)
args = parser.parse_args()

memcached_host = args.memcached_host
memcached_port = args.memcached_port
memcached_timeout_seconds = args.memcached_timeout_seconds
influxdb_host = args.influxdb_host
influxdb_port = args.influxdb_port
influxdb_db_name = args.influxdb_db_name
influxdb_user = args.influxdb_user
influxdb_password = args.influxdb_password
stats_interval_seconds = args.stats_interval_seconds

print """
m2i.py

memcached_host            : {}
memcached_port            : {}
memcached_timeout_seconds : {}
influxdb_host             : {}
influxdb_port             : {}
stats_interval_seconds    : {}
""".format(
    memcached_host,
    memcached_port,
    memcached_timeout_seconds,
    influxdb_host,
    influxdb_port,
    stats_interval_seconds)

influxdb_endpoint = "http://{}:{}/db/{}/series?u={}&p={}".format(
    influxdb_host,
    influxdb_port,
    influxdb_db_name,
    influxdb_user,
    influxdb_password)

# Global state
global_time = 0
global_total_requests = 0

def get_raw_stats():
    global memcached_host, memcached_port, memcached_timeout_seconds

    try:
        tn = telnetlib.Telnet(memcached_host,
            memcached_port,
            memcached_timeout_seconds)
        tn.write("stats\n")
        tn.write("quit\n")
        raw = tn.read_all()
        tn.close()
        return raw
    except:
        print "Failed to connect to memcached at [{}:{}]".format(
            memcached_host, memcached_port)
        return None

def parse_raw_stats(raw_stats):
    stats = {}
    lines = raw_stats.splitlines()
    for line in lines:
        parts = line.split()
        if len(parts) == 3 and parts[0] == "STAT":
            stat_name = parts[1]
            stat_value = parts[2]
            stats[stat_name] = stat_value

    # Available statistics:
    # =====================
    #
    # pid uptime time version libevent pointer_size rusage_user rusage_system
    # curr_connections total_connections connection_structures reserved_fds
    # cmd_get cmd_set cmd_flush cmd_touch get_hits get_misses delete_misses
    # delete_hits incr_misses incr_hits decr_misses decr_hits cas_misses
    # cas_hits cas_badval touch_hits touch_misses auth_cmds auth_errors
    # bytes_read bytes_written limit_maxbytes accepting_conns
    # listen_disabled_num threads conn_yields hash_power_level hash_bytes
    # hash_is_expanding malloc_fails bytes curr_items total_items
    # expired_unfetched evicted_unfetched evictions reclaimed
    # crawler_reclaimed crawler_items_checked lrutail_reflocked

    return stats

def extract_rps(raw_stats):
    global global_time, global_total_requests

    stats = parse_raw_stats(raw_stats)

    # Sum commands completed
    request_stats = ["cmd_get", "cmd_set", "cmd_flush", "cmd_touch"]
    total_requests = 0
    for r_stat in request_stats:
        total_requests += int(stats[r_stat])

    sample_time = int(stats["time"])

    # Copy previous values
    prev_time = global_time
    prev_total_requests = global_total_requests

    # Skip this iteration early if the sample has the previous timestamp
    if prev_time == sample_time:
        return None

    # Save current values
    global_time = sample_time
    global_total_requests = total_requests

    # Skip sample computation if this is the priming sample
    if prev_time == 0:
        return None

    # Compute requests per second
    delta_commands = global_total_requests - prev_total_requests
    delta_time = global_time - prev_time

    # Throw away nonsense values
    if delta_commands < 0 or delta_time < 0:
        return None

    requests_per_second = delta_commands / delta_time
    return requests_per_second

def post_to_influxdb(rps):
    global influxdb_endpoint, memcached_host, memcached_port

    memcached_instance = "{}:{}".format(memcached_host, memcached_port)

    print "Posting metrics to influxdb"
    samples = []

    rps_sample = {
        "name": "memcached_rps",
        "columns": [ "value", "memcached_instance" ],
        "points": [
            [rps, memcached_instance]
        ]
    }

    samples.append(rps_sample)

    request_json = json.dumps(samples)

    response = requests.post(url=influxdb_endpoint,
        data=request_json,
        headers={'Content-Type': 'application/octet-stream'})

    if response.status_code != 200:
        print "Received unexpected response [{}]: {}".format(
            response.status_code,
            response.text)

def collect_sample():
    print "Requesting stats from memcached at [{}:{}]".format(
        memcached_host,
        memcached_port)
    raw_stats = get_raw_stats()
    if raw_stats is None:
        print "Skipping requests-per-second report for this sample"
        return
    rps = extract_rps(raw_stats)
    if rps is None:
        print "Skipping requests-per-second report for this sample"
        return
    print "Reporting [{}] requests per second".format(rps)
    post_to_influxdb(rps)

def schedule(interval_seconds, f):
    def wrapped_f():
        f()
        timer = threading.Timer(interval_seconds, wrapped_f).start()
    threading.Timer(interval_seconds, wrapped_f).start()

# Schedule sample collection
print "Scheduling sample collection for every [{}] seconds".format(
    stats_interval_seconds)

schedule(stats_interval_seconds, collect_sample)

# Await Ctrl + C
signal.signal(signal.SIGINT, sys.exit(0))
signal.pause()
