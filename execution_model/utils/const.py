WORKLOAD_COLS_LIST = [
    'query_hash',
    'query_type',
    'bytes_scanned',
    'result_size',
    'write_volume',
    'cpu_time',
    'unique_db_instance',
    'num_read_tables',
    'intermediate_result_size',
    'hour',
    'read_tables',
    'write_table',
    'timestamp',
    'scan_to_result_ratio',
    'scan_to_i_result_ratio',
    'repetition_coefficient',
    'load',
]

WORKLOAD_TYPES_DICT = {
    'query_hash': "object",
    'query_type': "object",
    'bytes_scanned': 'int64',
    'result_size': 'int64',
    'write_volume': 'int64',
    'cpu_time': 'float64',
    'unique_db_instance': 'int64',
    'num_read_tables': 'int64',
    'intermediate_result_size': 'int64',
    'hour': 'int64',
    'read_tables': 'object',
    'write_table': 'object',
    'timestamp': 'datetime64[ns]',
    'scan_to_result_ratio': 'float64',
    'scan_to_i_result_ratio': 'float64',
    'repetition_coefficient': 'float64',
    'load': 'float64'
}

WORKLOAD_PLAN_COL_LIST = WORKLOAD_COLS_LIST +  [
    'cache_result',
    'cache_ir',
    'execution',
    'write_inc_table',
    'was_cached',
    'cache_writes',
    'cache_reads'
]

WORKLOAD_PLAN_TYPES = WORKLOAD_TYPES_DICT | {
    'cache_result': 'bool',
    'cache_ir': 'bool',
    'was_cached': 'bool',
    'cache_writes': 'int64',
    'cache_reads': 'int64',
    'load': 'float64',
    'execution': 'object',
    'write_inc_table': 'bool'
}

CACHE_COLS_LIST = WORKLOAD_COLS_LIST + ["size", "dirty", "delta"]

CACHE_TYPES_DICT = WORKLOAD_TYPES_DICT | {
    "size": "int64",
    "dirty": "bool",
    "delta": "int64"
}