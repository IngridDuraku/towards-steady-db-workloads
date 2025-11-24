def get_affected_queries_condition(query, workload):
    mask1 = workload["read_tables"].apply(lambda tables: query.write_table in tables)
    mask2 = workload["unique_db_instance"] == query.unique_db_instance

    return mask1 & mask2


def estimate_query_load(query, ref_values):
    weights = {
        "bytes_scanned": 0.8,
        "result_size": 0.5,
        "write_volume": 0.8,
        "cpu_time": 1.5,
    }

    bs_factor = 0 if ref_values["bytes_scanned"] == 0 else weights["bytes_scanned"] * query.bytes_scanned / ref_values["bytes_scanned"]
    rs_factor = 0 if ref_values["result_size"] == 0 else weights["result_size"] * query.result_size / ref_values["result_size"]
    wv_factor = 0 if ref_values["write_volume"] == 0 else weights["write_volume"] * query.write_volume / ref_values["write_volume"]
    cpu_factor = 0 if ref_values["cpu_time"] == 0 else weights["cpu_time"] * query.cpu_time # / ref_values["cpu_time"]

    load = bs_factor + rs_factor + wv_factor + cpu_factor

    return round(load, 2)