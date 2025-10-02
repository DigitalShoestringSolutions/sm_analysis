from functools import lru_cache, cache
import logging
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from pandas import DataFrame
from math import isnan


logger = logging.getLogger(__name__)


def write(config, measurement_name,timestamp_col = "_time",tag_cols = [], field_cols = []):
    influx_conf = config["influx"]
    bucket = influx_conf.get("bucket")

    async def wrapped(data_frame):
        async with InfluxDBClientAsync(
            url=influx_conf["url"], token=influx_conf["token"], org=influx_conf["org"]
        ) as client:
            write_api = client.write_api()

            line_protocol = data_frame_to_line_protocol(
                data_frame,
                measurement_name,
                timestamp_col=timestamp_col,
                tag_cols=tag_cols,
                field_cols=field_cols,
            )
            # logger.info(line_protocol)
            await write_api.write(bucket, influx_conf["org"], record=line_protocol)

        return data_frame

    return wrapped


def data_frame_to_line_protocol(
    data_frame: DataFrame, measurement, *, timestamp_col, tag_cols, field_cols
):
    serialized_dataframe = []
    for index, row in data_frame.iterrows():
        timestamp_as_int = row[timestamp_col].value
        tags_list = []
        for tag_name in tag_cols:
            tag_expr = f"{escape_tag_key(tag_name)}={escape_tag_value(row[tag_name])}"
            tags_list.append(tag_expr)
        comma_delimited_tags = ",".join(tags_list)

        fields_list = []
        for field_name in field_cols:
            esc_field_name = escape_field_key(field_name)
            esc_value = escape_field_value(str(row[field_name]))
            match row[field_name]:
                case str():
                    field_expr = f'{esc_field_name}="{esc_value}"'
                case int():
                    field_expr = f"{esc_field_name}={esc_value}i"
                case float():
                    if isnan(row[field_name]):
                        continue
                    field_expr = f"{esc_field_name}={esc_value}"
                case bool():
                    field_expr = f"{esc_field_name}={esc_value}"
                case None:
                    continue
            # logger.error(
            #     f">> {row[field_name]}   {type(row[field_name])}   ||    {field_expr}"
            # )
            fields_list.append(field_expr)
        if len(fields_list) == 0:
            continue
        comma_delimited_fields = ",".join(fields_list)

        line_protocol = f"{escape_measurement(measurement)},{comma_delimited_tags} {comma_delimited_fields} {timestamp_as_int}"
        serialized_dataframe.append(line_protocol.encode("utf-8"))
    return serialized_dataframe


def escape_measurement(string):
    return string.replace(" ", r"\ ").replace(",", r"\,")


def escape_tag_key(string:str):
    return string.replace(" ",r"\ ").replace("=",r"\=").replace(",",r"\,")


def escape_tag_value(string):
    return string.replace(" ", r"\ ").replace("=", r"\=").replace(",", r"\,")


def escape_field_key(string):
    return string.replace(" ", r"\ ").replace("=", r"\=").replace(",", r"\,")


def escape_field_value(string):
    return string.replace('"', r'\"').replace("\\", "\\\\") # can't use raw strings for \ and \\ because python's raw string implementation is stupid
