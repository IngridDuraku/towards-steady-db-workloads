import datetime
import math

import duckdb
import pandas as pd

redset_file_path = "data/full.parquet"
db_connection = duckdb.connect()

class RedsetWorkloadExtractor:
    def __init__(self, cluster_id, start_time=None, duration_h=24):
        self.cluster_id = cluster_id
        self.start_time = self.get_start_time(start_time)
        self.duration_h= duration_h
        self.end_time = self.get_end_time()
        self.cluster_data = self.load_from_redset()

    def get_start_time(self, start_time):
        if start_time is None:
            start_time = db_connection.execute(f"""
                SELECT median(arrival_timestamp) AS start_time
                FROM '{redset_file_path}'
                WHERE instance_id = {self.cluster_id}
            """).fetchdf()

        return start_time.iloc[0]["start_time"]

    def get_end_time(self):
        return self.start_time + datetime.timedelta(hours=self.duration_h)

    def load_from_redset(self):
        query = f"""
            SELECT * FROM '{redset_file_path}'
            WHERE instance_id = {self.cluster_id}
            AND arrival_timestamp between '{self.start_time}' and '{self.end_time}'
            ORDER BY arrival_timestamp ASC
            """
        cluster_data = db_connection.execute(query ).fetchdf()

        cluster_data["arrival_timestamp"] = pd.to_datetime(cluster_data["arrival_timestamp"])
        valid_q_type = cluster_data["query_type"].isin(["select", "insert", "update", "delete", "analyze"])
        cluster_data = cluster_data[valid_q_type]

        cluster_data.loc[cluster_data["query_type"] == "analyze", "query_type"] = "select"

        start_time = cluster_data["arrival_timestamp"].min()
        cluster_data["hour"] = ((cluster_data["arrival_timestamp"] - start_time).dt.total_seconds() // 3600 + 1).astype(
            int)
        cluster_data["read_table_ids"] = cluster_data["read_table_ids"].fillna("")
        cluster_data["write_table_ids"] = cluster_data["write_table_ids"].fillna("")

        return cluster_data

    def get_num_db(self):
        return self.cluster_data["database_id"].nunique()

    def get_num_tables(self, h=None):
        if not h:
            df = self.cluster_data
        else:
            mask = self.cluster_data["hour"] == h
            df = self.cluster_data.loc[mask]

        all_read_tables = df["read_table_ids"].str.split(",").explode()
        all_write_tables = df["write_table_ids"].str.split(",").explode()
        all_tables = pd.concat([all_read_tables, all_write_tables])
        unique_tables = all_tables.str.strip().unique()

        return len(unique_tables)

    def get_df_hourly(self, df):
        df_hourly = df.groupby(["hour"]).size().reset_index(name="load")
        max_hr = max(df_hourly["hour"].max() + 1, 25)
        if df.empty:
            max_hr = 25
        df_hourly.set_index("hour", inplace=True)
        df_hourly = df_hourly.reindex(range(1, max_hr), fill_value=0)
        df_hourly.reset_index(inplace=True)
        df_hourly.columns = ['hour', 'load']

        return df_hourly

    def extract_hourly_distributions(self):
        read_condition = self.cluster_data["query_type"].isin(["select"])
        read_queries = self.cluster_data[read_condition]
        df_hourly = self.get_df_hourly(read_queries)
        df_hourly["p"] = df_hourly["load"]
        df_hourly.drop('load', inplace=True, axis=1)
        df_hourly["p"] = df_hourly["p"] / len(read_queries) if len(read_queries) > 0 else 0
        df_hourly["tables_count"] = df_hourly.apply(lambda row: self.get_num_tables(row["hour"]), axis=1)
        df_hourly.set_index("hour", inplace=True)
        read_hourly_distributions = df_hourly.to_dict(orient="index")

        write_condition = ~read_condition
        write_queries = self.cluster_data[write_condition]
        df_hourly = self.get_df_hourly(write_queries)
        df_hourly["p"] = df_hourly["load"]
        df_hourly.drop('load', inplace=True, axis=1)
        df_hourly["p"] = df_hourly["p"] / len(write_queries) if len(write_queries) != 0 else 0
        df_hourly["tables_count"] = df_hourly.apply(lambda row: self.get_num_tables(row["hour"]), axis=1)
        df_hourly.set_index("hour", inplace=True)
        write_hourly_distributions = df_hourly.to_dict(orient="index")

        return read_hourly_distributions, write_hourly_distributions

    def estimate_bytes_scanned_bounds(self):
        lower_bound_mb, upper_bound_mb = self.cluster_data["mbytes_scanned"].quantile([.02,.97])

        return {
            "lower_bound_mb": lower_bound_mb,
            "upper_bound_gb": upper_bound_mb / 1e3
        }

    def get_read_tables_distribution(self):
        read_tables_counts = self.cluster_data['read_table_ids'].apply(lambda x: len(x.split(',')))
        read_table_counts = read_tables_counts.value_counts(normalize=True).sort_index().astype(float)

        return read_table_counts.to_dict()

    def max_read_tables_per_query(self):
        read_tables_counts = self.cluster_data['read_table_ids'].apply(lambda x: len(x.split(',')))

        return int(read_tables_counts.max())

    def estimate_repetitiveness(self):
        unique_exact_queries = self.cluster_data.drop_duplicates("feature_fingerprint")
        exact_repetitiveness = (len(self.cluster_data) - len(unique_exact_queries)) / len(self.cluster_data)

        return round(exact_repetitiveness, 4)

    def estimate_query_type_frequencies(self):
        num_insert_queries = len(self.cluster_data[self.cluster_data["query_type"] == "insert"])
        num_update_queries = len(self.cluster_data[self.cluster_data["query_type"] == "update"])
        num_delete_queries = len(self.cluster_data[self.cluster_data["query_type"] == "delete"])
        num_select_queries = len(self.cluster_data[self.cluster_data["query_type"] == "select"])

        size = len(self.cluster_data)

        return {
            "select": math.floor(num_select_queries / size * 1000) / 1000,
            "insert": math.floor(num_insert_queries / size * 1000) / 1000,
            "update": math.floor(num_update_queries / size * 1000) / 1000,
            "delete": math.floor(num_delete_queries / size * 1000) / 1000
        }

    def export_config(self, base_config):
        h_dist_r, h_dist_w = self.extract_hourly_distributions()

        return {
            "size": len(self.cluster_data),
            "query_config": {
                "query_type_p": self.estimate_query_type_frequencies(),
                "bytes_scanned": self.estimate_bytes_scanned_bounds(),
                "result_size": base_config["query_config"]["result_size"],
                "write_volume": base_config["query_config"]["write_volume"],
                "max_num_read_tables": self.max_read_tables_per_query(),
                "read_tables_distribution": self.get_read_tables_distribution(),
                "ir_scale": base_config["query_config"]["ir_scale"],
                "db_count": self.get_num_db(),
            },
            "scheduler_config": {
                "hourly_distribution_r": h_dist_r,
                "hourly_distribution_w": h_dist_w,
                "start_time": self.start_time.strftime('%Y-%m-%d %H:%M:%S'),
                "duration_h": self.duration_h,
                "table_count": self.get_num_tables()
            },
            "repetitiveness": self.estimate_repetitiveness(),
            "seed": base_config["seed"]
        }


