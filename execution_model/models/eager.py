import pandas as pd

from cache.repetition_aware import RepetitionAwareCache
from execution_model.models.base import BaseExecutionModel
from execution_model.utils.const import ExecutionTrigger


class EagerExecutionModel(BaseExecutionModel):
    def __init__(self, wl, cache_config):
        super().__init__(wl)
        self.cache_config = cache_config
        self.cache = RepetitionAwareCache(
            max_capacity=cache_config["max_capacity"],
            structure=wl.columns.tolist() + ["size"],
            types={
                **self.wl.dtypes.apply(lambda x: x.name).to_dict(),
                "size": "float64"
            },
            index_by="query_hash",
            cache_type=cache_config["cache_type"]
        )

    def generate_workload_execution_plan(self):
        if self.wl_execution_plan is None:
            ex_plan = []

            for _, query in self.wl.iterrows():
                is_read = query["query_type"] == "select"
                is_write = not is_read

                if is_write:
                    # add query for normal execution
                    query["cache_reads"] += 1 # count one cache read for retrieving affected queries
                    query["execution"] = "normal"
                    query["execution_trigger"] = ExecutionTrigger.IMMEDIATE.value
                    query["triggered_by"] = query["query_hash"]
                    ex_plan.append(query)

                    # add all affected queries for refresh
                    delta = query["write_volume"]
                    affected_queries = self.cache.get_affected_queries(query)

                    if len(affected_queries) > 0:
                        query["cache_writes"] = 1 # write all changes in bulk
                        affected_queries.loc[:, "bytes_scanned"] = delta
                        affected_queries.loc[:, "result_size"] = affected_queries["scan_to_result_ratio"] * delta
                        affected_queries.loc[:, "intermediate_result_size"] = affected_queries[
                                                                           "scan_to_i_result_ratio"] * delta

                        affected_queries.loc[:, "timestamp"] = query["timestamp"]
                        affected_queries.loc[:, "cache_result"] = True
                        affected_queries.loc[:, "cache_ir"] = True
                        affected_queries.loc[:, "execution"] = "incremental"
                        affected_queries.loc[:, "execution_trigger"] = ExecutionTrigger.TRIGGERED_BY_WRITE.value
                        affected_queries.loc[:, "triggered_by"] = query["query_hash"]

                        rows = [row for index, row in affected_queries.iterrows()]

                        ex_plan.extend(rows)

                    continue

                if query["query_hash"] in self.cache:
                    # add query as read from cache
                    query["was_cached"] = True
                    query["bytes_scanned"] = 0
                    query["cpu_time"] = 0
                    query["write_volume"] = 0
                    query["cache_reads"] += 1
                    query["execution"] = "incremental"
                    query["execution_trigger"] = ExecutionTrigger.IMMEDIATE.value
                    query["triggered_by"] = query["query_hash"]
                    ex_plan.append(query)
                else:
                    cached_query = query
                    cached_query["size"] = query["result_size"] + query["intermediate_result_size"]
                    is_cached = self.cache.put(
                        query["query_hash"],
                        cached_query,
                    )
                    if is_cached:
                        query["cache_ir"] = True
                        query["cache_result"] = True
                        query["write_delta"] = False
                        query["cache_writes"] += 1

                    query["execution"] = "normal"
                    query["execution_trigger"] = ExecutionTrigger.IMMEDIATE.value
                    query["triggered_by"] = query["query_hash"]
                    ex_plan.append(query)

            self.wl_execution_plan = pd.DataFrame(data=ex_plan)

        return self.wl_execution_plan

    def get_cost(self, hw_parameters):
        return super().get_cost(
            hw_parameters
        )