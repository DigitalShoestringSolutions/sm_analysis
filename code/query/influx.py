from functools import lru_cache, cache
import re
import datetime
import logging
import asyncio
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync


logger = logging.getLogger(__name__)

def current_and_voltage(config, dt_from ,dt_to):
    query = """
            from(bucket: _bucket)
                |> range(start: _start, stop: _stop)
                |> filter(fn: (r) => r["_measurement"] == "equipment_power_usage")
                |> filter(fn: (r) => r["_field"] == "current"or r["_field"] == "voltage" or r["_field"] == "power_real" or r["_field"] == "power_apparent")
                |> aggregateWindow(every: 5s, fn: mean, createEmpty: true, timeSrc:"_start")
                |> group(columns: ["machine","_field","_time"])
                |> sum()
                |> group(columns: ["machine"])
                |> pivot(columnKey: ["_field"], rowKey: ["_time"], valueColumn: "_value")
            """

    return do_query(
        config["influx"]["url"],
        config["influx"]["token"],
        config["influx"]["org"],
        query,
        _bucket=config["influx"].get("bucket"),
        _start=dt_from,
        _stop=dt_to,
    )


def latest_current_and_voltage(config, dt_to,machine):
    query = f"""
            from(bucket: _bucket)
                |> range(start: _start, stop: _stop)
                |> filter(fn: (r) => r["_measurement"] == "equipment_power_usage")
                |> filter(fn: (r) => r["_field"] == "current"or r["_field"] == "voltage" or r["_field"] == "power_real" or r["_field"] == "power_apparent")
                {'|> filter(fn: (r) => r["machine"] == _machine)' if machine else ""}
                |> aggregateWindow(every: 5s, fn: mean, createEmpty: true, timeSrc:"_start")
                |> group(columns: ["machine","_field"])
                |> last()
                |> group(columns: ["machine"])
                |> pivot(columnKey: ["_field"], rowKey: ["_time"], valueColumn: "_value")
            """

    return do_query(
        config["influx"]["url"],
        config["influx"]["token"],
        config["influx"]["org"],
        query,
        _bucket=config["influx"].get("bucket"),
        _start=dt_to - datetime.timedelta(minutes=1),
        _stop=dt_to,
        _machine = machine
    )


def real_power(config, dt_from, dt_to):
    query = """
            from(bucket: _bucket)
                |> range(start: _start, stop: _stop)
                |> filter(fn: (r) => r["_measurement"] == "equipment_power_usage")
                |> filter(fn: (r) => r["_field"] == "power_real")
                |> group(columns: ["machine"])
                |> pivot(columnKey: ["_field"], rowKey: ["_time"], valueColumn: "_value")
            """

    return do_query(
        config["influx"]["url"],
        config["influx"]["token"],
        config["influx"]["org"],
        query,
        _bucket = config["influx"].get("bucket"),
        _start = dt_from,
        _stop = dt_to,
    )

def energy(config, dt_from,dt_to,window,machine):
    query = f"""
            from(bucket: _bucket)
                |> range(start: _start, stop: _stop)
                |> filter(fn: (r) => r["_measurement"] == "energy")
                |> filter(fn: (r) => r["_field"] == "energy")
                {'|> filter(fn: (r) => r["machine"] == _machine)' if machine else ""}
                |> group()
                |> pivot(columnKey: ["_field"], rowKey: ["_time"], valueColumn: "_value")
                |> aggregateWindow(every: _window, fn: sum, createEmpty: true, timeSrc:"_start", column: "energy")
            """
    return do_query(
        config["influx"]["url"],
        config["influx"]["token"],
        config["influx"]["org"],
        query,
        _bucket=config["influx"].get("bucket"),
        _start=dt_from,
        _stop=dt_to,
        _window=Interval(window).timedelta,
        _machine=machine,
    )


def do_query(url, token, org, query, **params):
    async def wrapped(data):
        attempt_count = 1
        while True:
            try:
                async with InfluxDBClientAsync(url=url, token=token, org=org, timeout=60000) as client:
                    query_api = client.query_api()
                    data_frame = await query_api.query_data_frame(
                        query, params=params
                    )

                return data_frame
            except asyncio.TimeoutError:
                attempt_count += 1
                if attempt_count <= 3:
                    logger.warning(f"Retrying due to timeout during influx query - attempt {attempt_count}")
                else:
                    raise

    return wrapped


@lru_cache
def get_period_regex():
    return re.compile("(?P<number>\d*)(?P<unit>\w*)")


@lru_cache
def parse_period(period):
    regex = get_period_regex()
    match = regex.match(period)
    match_dict = match.groupdict()
    if "number" in match_dict and "unit" in match_dict:
        return int(match_dict["number"]), match_dict["unit"]
    else:
        raise Exception("Invalid period")


class Interval:
    multipliers = {
        "ms": 1,
        "s": 1000,
        "m": 60 * 1000,
        "h": 60 * 60 * 1000,
        "d": 24 * 60 * 60 * 1000,
        "w": 7* 24 * 60 * 60 * 1000,
    }

    def __init__(self, period_string) -> None:
        self.__number, self.__unit = parse_period(period_string)
        self.__normalised = self.__number * self.multipliers[self.__unit]

    def __str__(self) -> str:
        return f"{self.__number}{self.__unit}"

    def __lt__(self, other):
        return self.__normalised < other.__normalised

    def __gt__(self, other):
        return other < self

    @property
    def timedelta(self):
        return datetime.timedelta(milliseconds=self.__normalised)
