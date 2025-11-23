import datetime

import numpy as np
import pandas as pd


class QueryScheduler:
    def __init__(self, config, query_pool):
        self.config = config
        self.query_pool = query_pool
        nums = list(range(1, self.config["table_count"] + 1))
        self.table_pool = [str(num) + np.random.choice(["B"]) for num in nums]

    def get_random_timestamp_in_hour(self, hour):
        start = pd.Timestamp(self.config["start_time"])
        seconds_offset = np.random.randint(0, 3599) + hour * 3600
        return start + datetime.timedelta(seconds=seconds_offset)

    def assign_timestamps(self):
        def assign_read_tables(q):
            db_id = q["unique_db_instance"]
            table_pool = np.array(list(self.config["tables_read_access_dist"][db_id].keys()))
            if len(table_pool) == 0:
                table_pool = self.table_pool
            else:
                table_pool = table_pool + 'A'

            count = min(q["num_read_tables"], len(table_pool))
            read_table_p = np.array(list(self.config["tables_read_access_dist"][db_id].values()))
            read_table_p = [p / read_table_p.sum() for p in read_table_p]

            if len(read_table_p) == 0:
                read_table_p = [1/len(table_pool)] * len(table_pool) # uniform

            read_tables = np.random.choice(
                table_pool,
                count,
                p=read_table_p,
                replace=False
            )

            return ",".join(read_tables)

        def assign_write_table(q):
            db_id = q["unique_db_instance"]
            table_pool = np.array(list(self.config["tables_write_access_dist"][db_id].keys()))
            if len(table_pool) == 0:
                table_pool = self.table_pool
            else:
                table_pool = table_pool + 'A'

            write_table_p = np.array(list(self.config["tables_write_access_dist"][db_id].values()))
            write_table_p = [p / write_table_p.sum() for p in write_table_p]

            if len(write_table_p) == 0:
                write_table_p = [1/len(table_pool)] * len(table_pool) # uniform

            write_table = np.random.choice(
                table_pool,
                p=write_table_p,
                replace=False
            )

            return write_table

        hours = self.config["duration_h"] # hours of execution
        read_q_condition = self.query_pool["query_type"] == "select"
        write_q_condition = self.query_pool["query_type"] != "select"
        read_size = len(self.query_pool[read_q_condition])
        write_size = len(self.query_pool[write_q_condition])
        hourly_wl = pd.DataFrame()
        wl = self.query_pool.copy()

        for h in range(1, hours):
            read_q_count = int(self.config["hourly_distribution_r"][str(h)]["p"] * read_size)
            write_q_count = int(self.config["hourly_distribution_w"][str(h)]["p"] * write_size)

            read_queries = wl.loc[read_q_condition].sample(read_q_count)
            wl.drop(read_queries.index, inplace=True)

            write_queries = wl.loc[write_q_condition].sample(write_q_count)
            wl.drop(write_queries.index, inplace=True)

            if not read_queries.empty:
                read_queries["hour"] = h
                read_queries["read_tables"] = read_queries.apply(lambda q: assign_read_tables(q), axis=1)

                read_queries["write_table"] = None
                read_queries["timestamp"] = [self.get_random_timestamp_in_hour(h) for _ in range(read_q_count)]

            if not write_queries.empty:
                write_queries["hour"] = h
                write_queries["read_tables"] = write_queries.apply(lambda q: assign_read_tables(q), axis=1)

                write_queries["timestamp"] = [self.get_random_timestamp_in_hour(h) for _ in range(write_q_count)]
                write_queries["write_table"] = write_queries.apply(lambda q: assign_write_table(q), axis=1)

            hourly_wl = pd.concat([hourly_wl, read_queries, write_queries])

        workload = hourly_wl.sort_values("timestamp")

        return workload


