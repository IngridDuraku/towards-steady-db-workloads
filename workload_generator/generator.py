import numpy as np
import pandas as pd

from utils.workload import get_affected_queries_condition
from workload_generator.query_generator.query_generator import QueryGenerator
from workload_generator.scheduler.scheduler import QueryScheduler


class WorkloadGenerator:
    def __init__(self, config):
        self.config = config

    def generate_workload(self):
        np.random.seed(self.config["seed"])

        unique_queries_count, repetitions_count = self.get_unique_and_repeated_query_counts()

        # generate unique queries
        query_generator = QueryGenerator(self.config["query_config"])
        unique_queries = []
        for count in range(unique_queries_count):
            query = query_generator.generate_query()
            unique_queries.append(query)

        repetitions = np.random.choice(unique_queries, size=repetitions_count, replace=True).tolist()
        queries = unique_queries + repetitions
        np.random.shuffle(queries)
        query_pool = pd.DataFrame(queries)

        query_scheduler = QueryScheduler(self.config["scheduler_config"], query_pool)
        scheduled_queries = query_scheduler.assign_timestamps()

        workload = self.preprocess_workload(scheduled_queries)
        workload = self.calculate_repetition_coefficient(workload)

        return workload

    def preprocess_workload(self, workload):
        """
        Simulates dynamic execution of the query workload by changing bytes_scanned and result_sizes of repetitive queries
        based on incoming data ingestion (inserts, deletes, updates)
        :param workload: pd.DataFrame
        :return: pd.DataFrame with updated values
        """
        workload["scan_to_result_ratio"] = workload["result_size"] / workload["bytes_scanned"]
        workload["scan_to_i_result_ratio"] = workload["intermediate_result_size"] / workload["bytes_scanned"]
        workload["timestamp"] = pd.to_datetime(workload["timestamp"])
        workload["bytes_scanned"] = workload["bytes_scanned"].astype("int64")
        workload["result_size"] = workload["result_size"].astype("int64")
        workload["write_volume"] = workload["write_volume"].astype("int64")
        workload = workload.sort_values(by="timestamp")

        for query in workload.itertuples(index=True):
            if query.query_type == "select":
                continue

            affected_queries_condition = get_affected_queries_condition(query, workload) & (
                    workload["timestamp"] > query.timestamp)
            delta = query.write_volume
            delta_result = (workload[affected_queries_condition]["scan_to_result_ratio"] \
                           * delta).astype("int64")
            delta_i_result = (workload[affected_queries_condition]["scan_to_i_result_ratio"] \
                             * delta).astype("int64")

            if query.query_type in ("insert", "update"):  # not sure about update
                workload.loc[affected_queries_condition, "bytes_scanned"] += delta
                workload.loc[affected_queries_condition, "result_size"] += delta_result
                workload.loc[affected_queries_condition, "intermediate_result_size"] += delta_i_result

            elif query.query_type == "delete":
                workload.loc[affected_queries_condition, "bytes_scanned"] = (
                        workload.loc[affected_queries_condition, "bytes_scanned"] - delta
                ).clip(lower=10)

                workload.loc[affected_queries_condition, "result_size"] = (
                        workload.loc[affected_queries_condition, "result_size"] - delta_result
                ).clip(lower=7)

                workload.loc[affected_queries_condition, "intermediate_result_size"] = (
                        workload.loc[affected_queries_condition, "intermediate_result_size"] - delta_i_result
                ).clip(lower=5)

        return workload

    def get_unique_and_repeated_query_counts(self):
        repetitiveness = self.config["repetitiveness"]
        wl_size = self.config["size"]
        repetitions_count = int(wl_size * repetitiveness)
        unique_queries_count = wl_size - repetitions_count

        return int(unique_queries_count), repetitions_count

    def calculate_repetition_coefficient(self, workload):
        workload["repetition_coefficient"] = (
                                 workload.groupby("query_hash")["query_hash"].transform("count") - 1
                             ) / len(workload)

        return workload

