import pandas as pd

from cache.repetition_aware import RepetitionAwareCache
from execution_model.models.base import BaseExecutionModel
from execution_model.utils.dependency_graph import DependencyGraph
from utils.workload import estimate_query_load


class HybridExecutionModel(BaseExecutionModel):
    def __init__(self, wl, cache_config):
        super().__init__(wl)
        self.cache_config = cache_config
        self.cache = RepetitionAwareCache(
            max_capacity=cache_config["max_capacity"],
            structure=wl.columns.tolist() + ["size", "dirty"],
            types={
                **self.wl.dtypes.apply(lambda x: x.name).to_dict(),
                "size": "float64",
                "dirty": "bool"
            },
            index_by="query_hash"
        )
        self.dependency_graph = DependencyGraph(
            pd.DataFrame({}, columns=wl.columns.tolist() + ["id"])
        )
        self.load_ref = {
            "bytes_scanned": self.wl["bytes_scanned"].median(),
            "result_size": self.wl["result_size"].median(),
            "write_volume": self.wl["write_volume"].median(),
            "cpu_time": 1 / self.wl["cpu_time"].median()
        }
        self.set_execution_hour()
        self.current_hour = 1
        self.load_threshold = self.get_load_threshold()
        self.hourly_load = { str(i): 0 for i in range(1, max(self.wl["hour"]) + 1) }

    def get_load_threshold(self):
        self.wl["load"] = self.wl.apply(lambda query: estimate_query_load(query, self.load_ref), axis=1)
        load_threshold = self.wl["load"].quantile(0.5)

        return 1.1 * load_threshold # 10% tolerance

    def get_affected_queries(self, write_query):
        delta = write_query["write_volume"]
        affected_queries = self.cache.get_affected_queries(write_query)
        write_query["cache_reads"] += 1  # count one cache read for retrieving affected queries

        if affected_queries.empty:
            return affected_queries

        affected_queries.loc["bytes_scanned"] = delta
        affected_queries.loc["result_size"] = affected_queries["scan_to_result_ratio"] * delta
        affected_queries.loc["intermediate_result_size"] = affected_queries[
                                                               "scan_to_i_result_ratio"] * delta

        affected_queries.loc["timestamp"] = write_query.timestamp
        affected_queries.loc["was_cached"] = False
        affected_queries.loc["cache_result"] = True
        affected_queries.loc["cache_ir"] = True
        affected_queries.loc["write_inc_table"] = False
        affected_queries.loc["cache_writes"] += 1
        affected_queries.loc["execution"] = "incremental"

        affected_queries.loc["load"] = affected_queries.apply(
            lambda query: estimate_query_load(query, self.load_ref), axis=1
        )

        return affected_queries


    def set_execution_hour(self):
        start_time = self.wl["start_time"].min()
        self.wl["hour"] = ((self.wl["timestamp"] - start_time).dt.total_seconds() // 3600 + 1).astype(int)

    def has_capacity(self, h, q):
        plan = self.get_plan(q)
        total_load = plan["load"].sum() + q["load"]
        if total_load + self.load_threshold >= self.hourly_load[str(h)]:
            return True

        return False

    def get_plan(self, q):
        qid = self.dependency_graph.add_query(q)
        dependencies = self.dependency_graph.get_all_dependencies(qid)
        dependencies = self.dependency_graph.df[self.dependency_graph.df["id"].isin(dependencies)]
        dependencies = dependencies.sort_values(by=["timestamp"])

        plan = pd.DataFrame()

        for _, update in dependencies:
            affected_queries = self.get_affected_queries(update)
            plan = pd.concat([plan, update, affected_queries])

        self.dependency_graph.remove(qid)

        return plan


    def generate_workload_execution_plan(self):
        if self.wl_execution_plan is None:
            ex_plan = []

            for _, query in self.wl.iterrows():
                is_read = query["query_type"] == "select"
                is_write = ~is_read

                h = query["hour"]
                if h > self.current_hour:
                    # TODO: check if there is still capacity in the current hour to schedule pending writes
                    current_hour_load = sum([estimate_query_load(q, self.load_ref) for q in ex_plan])
                    capacity_left = self.load_threshold - current_hour_load
                    if capacity_left > 0:
                        pass

                    self.current_hour += 1

                if is_write:
                    # TODO:
                    #  1) get dependencies
                    #  2) check if there is capacity to execute dependencies + this query
                    #  3) refresh affected queries (incremental run) - based on query runtime estimation
                    if self.has_capacity(h, query):
                        plan = self.get_plan(query)
                        ex_plan.extend(plan)
                    else:
                        self.dependency_graph.add_query(query)

                    continue

                qid = self.dependency_graph.add_query(query)
                deps = self.dependency_graph.get_all_dependencies(qid)
                pending_updates = self.dependency_graph.df[self.dependency_graph.df["id"].isin(deps)]
                pending_updates = pending_updates.sort_values(by=["timestamp"])

                if query["query_hash"] in self.cache:
                    if pending_updates.empty:
                        # TODO: read from cache
                        query["was_cached"] = True
                        query["cache_writes"] = 0
                        query["bytes_scanned"] = 0
                        query["cpu_time"] = 0
                        query["write_volume"] = 0
                        query["cache_reads"] += 1
                        query["execution"] = "incremental"

                        ex_plan.append(query)
                        self.dependency_graph.remove(qid)
                        continue

                    # TODO: execute incrementally
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
                    self.dependency_graph.remove(qid)
                else:
                    # need to execute from scratch & scan all tables
                    # 1 => refresh all entries for pending queries
                    for _, wq in pending_updates.iterrows():
                        affected_queries = self.get_affected_queries(wq)
                        rows = [row for index, row in affected_queries.iterrows()]
                        ex_plan.extend(rows)

                    # 2 => need to run all pending writes + dependencies
                    pending_updates["timestamp"] = query["timestamp"]
                    pending_updates["execution"] = "normal"
                    pending_updates["was_cached"] = False
                    pending_updates["cache_result"] = False
                    pending_updates["cache_ir"] = False
                    pending_updates["write_inc_table"] = False
                    rows = [row for index, row in pending_updates.iterrows()]
                    ex_plan.extend(rows)

                    # 3 => drop pending updates and this query from dep graph
                    self.dependency_graph.remove_with_dependencies(qid)

                    # 4: => run query and save in cache
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

        return self.wl_execution_plan
