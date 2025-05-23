def get_affected_queries_condition(query, workload):
    mask1 = workload["read_tables"].apply(lambda tables: query.write_table in tables)
    mask2 = workload["unique_db_instance"] == query.unique_db_instance

    return mask1 & mask2


def estimate_query_load(query, ref_values):
    weights = {
        "bytes_scanned": 0.8,
        "result_size": 0.5,
        "write_volume": 1.2,
        "cpu_time": 1.5,
    }

    load = (weights["bytes_scanned"] * query.bytes_scanned / ref_values["bytes_scanned"] +
            weights["result_size"] * query.result_size / ref_values["result_size"] +
            weights["write_volume"] * query.write_volume / ref_values["write_volume"] +
            weights["cpu_time"] * query.cpu_time / ref_values["cpu_time"])

    return round(load, 2)