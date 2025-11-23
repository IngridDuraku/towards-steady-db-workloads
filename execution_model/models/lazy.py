from datetime import timedelta

import pandas as pd

from cache.repetition_aware import RepetitionAwareCache
from execution_model.models.base import BaseExecutionModel
from execution_model.utils.const import ExecutionTrigger, CACHE_COLS_LIST, CACHE_TYPES_DICT, WORKLOAD_PLAN_COL_LIST
from execution_model.utils.dependency_graph import DependencyGraph


class LazyExecutionModel(BaseExecutionModel):
    def __init__(self, wl, cache_config):
        super().__init__(wl)
        self.cache_config = cache_config
        self.cache = RepetitionAwareCache(
            max_capacity=cache_config["max_capacity"],
            structure=CACHE_COLS_LIST,
            types=CACHE_TYPES_DICT,
            index_by="query_hash",
            cache_type=cache_config["cache_type"]
        )
        self.dependency_graph = DependencyGraph(
            pd.DataFrame({}, columns=wl.columns.tolist() + ["id"])
        )
        self.wl_execution_plan = pd.DataFrame(
            columns=WORKLOAD_PLAN_COL_LIST
        )

    def generate_workload_execution_plan(self):
        if self.wl_execution_plan.empty:
            for _, query in self.wl.iterrows():
                is_read = query["query_type"] == "select"
                is_write = not is_read

                if is_write:
                    self.dependency_graph.add_query(query)
                    continue

                qid = self.dependency_graph.add_query(query)
                pending_updates = self.dependency_graph.get_all_dependencies(qid)

                if not pending_updates.empty:
                    pending_updates.drop(columns="id", inplace=True)
                    pending_updates.loc[:, "timestamp"] = query["timestamp"]
                    pending_updates.loc[:, "hour"] = query["hour"]
                    pending_updates.loc[:, "execution"] = "normal"
                    pending_updates.loc[:, "execution_trigger"] = ExecutionTrigger.TRIGGERED_BY_READ.value
                    pending_updates.loc[:, "triggered_by"] = query["query_hash"]
                    query["was_cached"] = False
                    query["cache_result"] = False
                    query["cache_ir"] = False
                    query["write_delta"] = False


                    if not self.cache.cache.empty:
                        write_tables = set(pending_updates["write_table"])
                        affected_queries_mask = self.cache.cache.apply(
                            lambda q: len(set(q["read_tables"].split(",")) & write_tables) > 0,
                            axis=1
                        )
                        self.cache.cache.loc[affected_queries_mask, "dirty"] = True
                        self.cache.cache.loc[affected_queries_mask, "delta"] = pending_updates["write_volume"].sum()
                        pending_updates["write_delta"] = True
                        query.loc["cache_writes"] = 1

                    self.wl_execution_plan = pd.concat([self.wl_execution_plan, pending_updates], ignore_index=True)

                self.dependency_graph.remove_with_dependencies(qid)

                query.loc["cache_reads"] += 1
                if query["query_hash"] in self.cache:
                    cached_query = self.cache.get(query["query_hash"])
                    if cached_query["dirty"]:
                        scan_delta = cached_query["delta"]
                        result_delta = query["scan_to_result_ratio"] * scan_delta
                        i_result_delta = query["scan_to_i_result_ratio"] * scan_delta

                        query.loc["bytes_scanned"] = scan_delta
                        query.loc["result_size"] = result_delta
                        query.loc["intermediate_result_size"] = i_result_delta

                        query.loc["was_cached"] = False
                        query.loc["write_delta"] = False
                        query.loc["size"] = query["result_size"] + query["intermediate_result_size"]
                        query.loc["dirty"] = False
                        query.loc["delta"] = 0
                        query.loc["timestamp"] = query["timestamp"]
                        query.loc["hour"] = query["hour"]

                        is_cached = self.cache.put(query["query_hash"], query)

                        if is_cached:
                            query.loc["cache_ir"] = True
                            query.loc["cache_result"] = True
                            query.loc["cache_writes"] += 1
                    else:
                        query.loc["was_cached"] = True
                        query.loc["bytes_scanned"] = 0
                        query.loc["cpu_time"] = 0
                        query.loc["write_volume"] = 0

                    query.loc["execution"] = "incremental"
                    query.loc["execution_trigger"] = ExecutionTrigger.IMMEDIATE.value
                    self.wl_execution_plan.loc[len(self.wl_execution_plan)] = query
                else:
                    # run from scratch
                    cached_query = query
                    cached_query["size"] = query["result_size"] + query["intermediate_result_size"]
                    cached_query["delta"] = 0
                    cached_query["dirty"] = False

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

                    self.wl_execution_plan.loc[len(self.wl_execution_plan)] = query

            hour = self.wl_execution_plan["hour"].max() + 1
            timestamp = self.wl_execution_plan["timestamp"].max() + timedelta(hours=1)

            pending_queries = self.dependency_graph.df
            if not pending_queries.empty:
                pending_queries.drop(columns="id", inplace=True)
                pending_queries.loc[:, "timestamp"] = timestamp
                pending_queries.loc[:, "hour"] = hour
                pending_queries.loc[:, "execution"] = "normal"
                pending_queries.loc[:, "execution_trigger"] = ExecutionTrigger.PENDING.value
                pending_queries.loc[:, "triggered_by"] = None

                if not self.cache.cache.empty:
                    write_tables = set(pending_queries["write_table"])
                    affected_queries_mask = self.cache.cache.apply(
                        lambda q: len(set(q["read_tables"].split(",")) & write_tables) > 0,
                        axis=1
                    )
                    self.cache.cache.loc[affected_queries_mask, "dirty"] = True
                    self.cache.cache.loc[affected_queries_mask, "delta"] = pending_queries["write_volume"].sum()
                    pending_queries["write_delta"] = True

                self.wl_execution_plan = pd.concat([self.wl_execution_plan, pending_queries], ignore_index=True)

        return self.wl_execution_plan

    def get_cost(self, hw_parameters):
       return super().get_cost(
           hw_parameters,
       )


