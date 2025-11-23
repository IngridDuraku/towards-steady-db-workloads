import numpy as np


class WorkloadInsights:
    def __init__(self, wl):
        self.wl = wl
        self.wl_size = len(wl)
        self.wl_hourly = self.get_hourly_load()
        self.repetitiveness = self.estimate_repetitiveness()
        self.query_type_frequencies = self.estimate_query_type_frequencies()
        self.spikiness = self.estimate_spikiness()
        self.std_dev = self.estimate_std_dev()

    def get_insights(self):
        return {
            "size": self.wl_size,
            "repetitiveness": self.repetitiveness,
            "query_type_frequencies": self.query_type_frequencies,
            "spikiness": self.spikiness,
            "std_dev": self.std_dev
        }

    def get_hourly_load(self):
        df = self.wl.copy()
        df_hourly = df.groupby(["hour"])["load"].sum().reset_index(name="load")
        max_hr = max(df_hourly["hour"].max() + 1, 25)
        df_hourly.set_index("hour", inplace=True)
        df_hourly = df_hourly.reindex(range(1, int(max_hr)), fill_value=0)
        df_hourly.reset_index(inplace=True)
        df_hourly.columns = ['hour', 'load']

        return df_hourly

    def estimate_spikiness(self):
        df_hourly = self.wl_hourly.copy()
        max_load = df_hourly["load"].max()
        min_load = 0

        df_hourly["load"] = (df_hourly["load"] - min_load) / (max_load - min_load)

        df_hourly['prev_load'] = df_hourly['load'].shift(1)
        df_hourly['squared_diff'] = (df_hourly['load'] - df_hourly['prev_load']) ** 2
        df_hourly = df_hourly.dropna(subset=['squared_diff'])
        rmse = np.sqrt(df_hourly['squared_diff'].mean())

        return rmse

    def estimate_repetitiveness(self):
        unique_exact_queries = self.wl.drop_duplicates("query_hash")
        exact_repetitiveness = (self.wl_size - len(unique_exact_queries)) / self.wl_size

        return round(exact_repetitiveness, 4)

    def estimate_query_type_frequencies(self):
        num_insert_queries = len(self.wl[self.wl["query_type"] == "insert"])
        num_update_queries = len(self.wl[self.wl["query_type"] == "update"])
        num_delete_queries = len(self.wl[self.wl["query_type"] == "delete"])
        num_select_queries = len(self.wl[self.wl["query_type"] == "select"])

        return {
            "select": round(num_select_queries / self.wl_size, 4),
            "insert": round(num_insert_queries / self.wl_size, 4),
            "update": round(num_update_queries / self.wl_size, 4),
            "delete": round(num_delete_queries / self.wl_size, 4),
        }

    def estimate_std_dev(self):
        df_hourly = self.wl_hourly.copy()

        max_load = df_hourly["load"].max()
        min_load = 0

        df_hourly["load"] = (df_hourly["load"] - min_load) / (max_load - min_load)
        df_hourly['squared_diff'] = (df_hourly['load'] - df_hourly["load"].mean()) ** 2

        df_hourly = df_hourly.dropna(subset=['squared_diff'])
        std_dev = np.sqrt(df_hourly['squared_diff'].mean())

        return std_dev

