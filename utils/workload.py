def get_affected_queries_condition(query, workload):
    mask1 = workload["read_tables"].apply(lambda tables: query.write_table in tables)
    mask2 = workload["unique_db_instance"] == query.unique_db_instance

    return mask1 & mask2