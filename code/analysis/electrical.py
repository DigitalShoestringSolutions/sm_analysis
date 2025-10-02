import logging
from pandas import DataFrame

logger = logging.getLogger(__name__)


def calculate_power(config):
    default_voltage = config["voltage_line_neutral"].get("default", 230)
    machine_voltages = config["voltage_line_neutral"].get("machines", {})

    default_power_factor = config["power_factor"].get("default", 1.0)
    machine_power_factors = config["power_factor"].get("machines", {})

    async def wrapped(data_frame: DataFrame):

        # logger.info(data_frame.to_string())
        for index,row in data_frame.iterrows():
            if "power_apparent" in row and "power_real" in row:
                data_frame.drop(index)  # values already there - no need to calculate

            machine = row["machine"]
            if "power_apparent" not in row or row["power_apparent"] is None:
                voltage = (
                    row["voltage"]
                    if "voltage" in row
                    else machine_voltages.get(machine, default_voltage)
                )
                power_apparent = (
                    row["current"] * voltage if row["current"] is not None else None
                )
                data_frame.at[index, "power_apparent"] = power_apparent
            else:
                power_apparent = row["power_apparent"]

            if "power_real" not in row or row["power_real"] is None:
                power_factor = machine_power_factors.get(machine, default_power_factor)

                data_frame.at[index, "power_real"] = (
                    power_apparent * power_factor
                    if power_apparent is not None
                    else None
                )
        # logger.info(data_frame.to_string())
        return data_frame

    return wrapped


from scipy import integrate
def calculate_energy(config):

    async def wrapped(data_frame: DataFrame):
        result = []
        if len(data_frame)>0:
            for group_keys, df in data_frame.groupby(["machine"]):
                # logger.info(f"{group_keys},{df}")
                if "power_real" in df:
                    df["_time_seconds"] = df["_time"].astype("int64") // 10**9
                    # df["_time_seconds"] = df["_time"].timestamp()
                    power_integral = integrate.trapezoid(
                        df["power_real"], x=df["_time_seconds"]
                    )
                else:
                    logger.warning("this shouldn't happen")
                    # power_integral = integrate.trapezoid(df["current"], x=df["_time"])

                energy_wh = power_integral/3600 # seconds in hour
                result.append(
                    {
                        "machine": group_keys[0],
                        "energy": energy_wh,
                        "_time": df["_time"].iat[-1],
                    }
                )
            output_df = DataFrame(result)
            # logger.info(output_df)
            return output_df
        else:
            raise Exception("No data in dataframe")

    return wrapped
