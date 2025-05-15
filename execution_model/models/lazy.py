import pandas as pd

from cache.repetition_aware import RepetitionAwareCache
from execution_model.models.base import BaseExecutionModel
from execution_model.utils.dependency_graph import DependencyGraph


class LazyExecutionModel(BaseExecutionModel):
    def __init__(self, wl, cache_config):
        super().__init__(wl)
        self.cache_config = cache_config
        self.cache = RepetitionAwareCache(
            max_capacity=cache_config["max_capacity"],
            structure=wl.columns.tolist() + ["size"],
            types={
                **self.wl.dtypes.apply(lambda x: x.name).to_dict(),
                "size": "float64",
            },
            index_by="query_hash"
        )
        self.dependency_graph = DependencyGraph(
            pd.DataFrame({}, columns=wl.columns.tolist() + ["id"])
        )

    def generate_workload_execution_plan(self):
        if self.wl_execution_plan is None:
            ex_plan = []

            for _, query in self.wl.iterrows():
                is_read = query["query_type"] == "select"
                is_write = ~is_read

                if is_write:
                    self.dependency_graph.add_query(query)
                    continue

                qid = self.dependency_graph.add_query(query)
                deps = self.dependency_graph.get_all_dependencies(qid)
                pending_updates = self.dependency_graph.df[self.dependency_graph.df["id"].isin(deps)]

                if pending_updates.empty:
                    if query["query_hash"] in self.cache:
                        query["was_cached"] = True
                        query["cache_writes"] = 0
                        query["bytes_scanned"] = 0
                        query["cpu_time"] = 0
                        query["write_volume"] = 0
                        query["cache_reads"] += 1
                        query["execution"] = "incremental"

                        ex_plan.append(query)
                        self.dependency_graph.remove_with_dependencies(qid)
                        continue
                else:
                    pending_updates["timestamp"] = query["timestamp"]
                    pending_updates["execution"] = "normal"
                    query["was_cached"] = False
                    query["cache_result"] = False
                    query["cache_ir"] = False
                    query["write_inc_table"] = False

                    rows = [row for index, row in pending_updates.iterrows()]
                    ex_plan.extend(rows)

                self.dependency_graph.remove_with_dependencies(qid)

                if query["query_hash"] in self.cache:
                    last_occ = self.cache.get(query.query_hash)
                    scan_delta = abs(query["bytes_scanned"] - last_occ["bytes_scanned"])
                    result_delta = abs(query["result_size"] - last_occ["result_size"])
                    i_result_delta = abs(query["intermediate_result_size"] - last_occ["intermediate_result_size"])

                    query["bytes_scanned"] = scan_delta
                    query["result_size"] = result_delta
                    query["intermediate_result_size"] = i_result_delta

                    query["was_cached"] = False
                    query["cache_result"] = True
                    query["cache_ir"] = True
                    query["write_inc_table"] = False
                    query["cache_reads"] += 1  # retrieve last_occ

                    re_cached_query = query
                    re_cached_query["size"] = query["result_size"] + query["intermediate_result_size"]

                    is_cached = self.cache.put(query.query_hash, re_cached_query)

                    if is_cached:
                        query["cache_ir"] = True
                        query["cache_result"] = True
                        query["cache_writes"] += 1

                    query["execution"] = "incremental"
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
                        query["write_inc_table"] = False
                        query["cache_writes"] += 1

                    query["execution"] = "normal"
                    ex_plan.append(query)

    def get_cost(self, hw_parameters):
       return super().get_cost(
           hw_parameters,
       )


