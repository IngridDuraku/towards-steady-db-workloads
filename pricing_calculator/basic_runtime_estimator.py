import numpy as np

from pricing_calculator.const import GiB_TO_BYTES, S3_NETWORK_SPEED_SCALE


class BasicRuntimeEstimator:
    @staticmethod
    def estimate_runtime_per_query(hw_parameters, wl):
        network_speed = hw_parameters["instance"]["network_speed"] * GiB_TO_BYTES * S3_NETWORK_SPEED_SCALE * 0.8
        if hw_parameters["cache"]["type"] == "s3":
            cache_speed = network_speed
        else:
            cache_speed = hw_parameters["cache"]["throughput_mb_per_s"] * 10e6

        # cost components
        # 1- Query runtime
        cpu_time = wl["cpu_time"] / hw_parameters["instance"]["vCPUs"]

        network_speed_map = {
            "incremental": cache_speed,
            "normal": network_speed
        }

        wl["network_speed"] = wl["execution"].map(network_speed_map)
        network_time = (wl["bytes_scanned"] + wl["write_volume"]) / wl["network_speed"]

        # is_write = wl["query_type"].isin(["insert", "delete", "update"])
        # wl.loc[:, "db_latency"] = pd.Series(np.random.uniform(
        #     HW_PARAMETERS["cache"]["s3"]["request_latency_min"] / 1000,
        #     HW_PARAMETERS["cache"]["s3"]["request_latency_max"] / 1000,
        #     len(wl)
        # ))
        # wl.loc[is_write, "db_latency"] *= 2

        cache_latency = (wl["cache_reads"] + wl["cache_writes"]) * np.random.uniform(
            hw_parameters["cache"]["request_latency_min"] / 1000,
            hw_parameters["cache"]["request_latency_max"] / 1000,
            len(wl)
        )

        # 2- Time spent for caching
        write_cache_bytes = wl["cache_result"] * wl["result_size"]
        write_cache_bytes += wl["cache_ir"] * wl["intermediate_result_size"]
        write_cache_bytes += wl["write_delta"] * wl["write_volume"]

        read_cache_bytes = wl["was_cached"] * wl["result_size"]

        cache_time = (write_cache_bytes + read_cache_bytes) / cache_speed + cache_latency

        return cpu_time + network_time + cache_time # + wl["db_latency"]

    @staticmethod
    def get_wl_total_runtime(hw_parameters, wl):
        wl["total_runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters, wl)

        return wl["total_runtime"].sum()

