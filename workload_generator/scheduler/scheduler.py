import datetime

import numpy as np
import pandas as pd


class QueryScheduler:
    def __init__(self, config, query_pool):
        self.config = config
        self.query_pool = query_pool
        nums = list(range(1, self.config["table_count"] + 1))
        self.table_pool = [str(num) + np.random.choice(["A", "B", "C"]) for num in nums]

    def get_random_timestamp_in_hour(self, hour):
        start = pd.Timestamp(self.config["start_time"]).date()
        seconds_offset = np.random.randint(0, 3599) + hour * 3600
        return start + datetime.timedelta(seconds=seconds_offset)

    def assign_timestamps(self):
        hours = self.config["duration_h"] # hours of execution
        read_q_condition = self.query_pool["query_type"] == "select"
        write_q_condition = self.query_pool["query_type"] != "select"
        read_size = len(self.query_pool[read_q_condition])
        write_size = len(self.query_pool[write_q_condition])
        hourly_wl = pd.DataFrame()
        wl = self.query_pool.copy()

        for h in range(1, hours):
            read_q_count = int(self.config["hourly_distribution_r"][h]["p"] * read_size)
            write_q_count = int(self.config["hourly_distribution_w"][h]["p"] * write_size)

            read_queries = wl.loc[read_q_condition].sample(read_q_count)
            wl.drop(read_queries.index, inplace=True)

            write_queries = wl.loc[write_q_condition].sample(write_q_count)
            wl.drop(write_queries.index, inplace=True)

            read_queries["hour"] = h
            read_tables = np.random.choice(
                self.table_pool,
                self.config["hourly_distribution_r"][h]["tables_count"]
            )
            read_queries["read_tables"] = [
                ",".join(np.random.choice(read_tables, np.random.randint(1, len(read_tables) + 1), replace=False))
                for _ in range(read_q_count)
            ]
            read_queries["write_table"] = None
            read_queries["timestamp"] = [self.get_random_timestamp_in_hour(h) for _ in range(read_q_count)]

            write_queries["hour"] = h
            read_tables = np.random.choice(
                self.table_pool,
                self.config["hourly_distribution_w"][h]["tables_count"]
            )
            write_queries["read_tables"] = [
                ",".join(np.random.choice(read_tables, np.random.randint(1, len(read_tables) + 1), replace=False))
                for _ in range(write_q_count)
            ]
            write_queries["timestamp"] = [self.get_random_timestamp_in_hour(h) for _ in range(write_q_count)]
            write_queries["write_table"] = [np.random.choice(self.table_pool) for _ in range(write_q_count)]

            hourly_wl = pd.concat([hourly_wl, read_queries, write_queries])

        workload = hourly_wl.sort_values("timestamp")

        return workload


