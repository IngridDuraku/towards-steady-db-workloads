from pricing_calculator.const import GiB_TO_BYTES, S3_NETWORK_SPEED_SCALE


class BasicRuntimeEstimator:
    @staticmethod
    def estimate_runtime_per_query(hw_parameters, wl):
        network_speed = hw_parameters["instance"]["network_speed"] * GiB_TO_BYTES * S3_NETWORK_SPEED_SCALE
        if hw_parameters["cache"]["type"] == "s3":
            network_speed *= 0.8

        # cost components
        # 1- Query runtime
        cpu_time = wl["cpu_time"] / hw_parameters["instance"]["vCPUs"]
        network_time = (wl["bytes_scanned"] + wl["write_volume"]) / network_speed

        # 2- Time spent for caching
        write_cache_bytes = wl["cache_result"] * wl["result_size"]
        write_cache_bytes += wl["cache_ir"] * wl["intermediate_result_size"]
        write_cache_bytes += wl["write_inc_table"] * wl["write_volume"]

        read_cache_bytes = wl["was_cached"] * wl["result_size"]

        cache_time = (write_cache_bytes + read_cache_bytes) / network_speed

        return cpu_time + network_time + cache_time

    @staticmethod
    def get_wl_total_runtime(hw_parameters, wl):
        wl["total_runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters, wl)

        return wl["total_runtime"].sum()

