import pandas as pd

from cache.repetition_aware import RepetitionAwareCache
from execution_model.models.base import BaseExecutionModel
from execution_model.utils.dependency_graph import DependencyGraph
from utils.workload import estimate_query_load


class IncHybridModel(BaseExecutionModel):
    def __init__(self, wl, cache_config, load_ref):
        super().__init__(wl)
        self.cache_config = cache_config
        self.cache = RepetitionAwareCache(
            max_capacity=cache_config["max_capacity"],
            structure=wl.columns.tolist() + ["size", "dirty", "delta"],
            types={
                **self.wl.dtypes.apply(lambda x: x.name).to_dict(),
                "size": "float64",
                "dirty": "bool",
                "delta": "int64",
            },
            index_by="query_hash"
        )
        self.dependency_graph = DependencyGraph(
            pd.DataFrame({}, columns=wl.columns.tolist() + ["load", "id"])
        )
        self.load_ref = load_ref
        # self.set_execution_hour()
        self.current_hour = 1
        self.load_threshold = self.get_load_threshold()
        self.hourly_load = { str(i): 0 for i in range(1, max(self.wl["hour"]) + 1) }
        self.wl_execution_plan = None

    def get_load_threshold(self):
        self.wl["load"] = self.wl.apply(lambda query: estimate_query_load(query, self.load_ref), axis=1)
        df_hr = self.wl.groupby(["hour"])["load"].sum().reset_index(name="load")
        load_threshold = df_hr["load"].quantile(0.5)

        return 0.7 * load_threshold # 10% tolerance

    def execute_write(self, query, timestamp):
        # add to dependency graph
        qid = self.dependency_graph.add_query(query)
        dependencies = self.dependency_graph.get_all_dependencies(qid)
        # check if there is capacity
        required_capacity = query["load"] + dependencies["load"]
        if self.load_threshold - self.hourly_load[str(self.current_hour)] < required_capacity:
            queries_plan = pd.DataFrame()
            queries_plan.loc[: len(dependencies) - 1] = dependencies
            queries_plan.loc[:, "timestamp"] = timestamp
            queries_plan.loc[:, "hour"] = self.current_hour
            queries_plan.loc[:, "execution"] = "normal"
            queries_plan.loc[:, "was_cached"] = False
            queries_plan.loc[:, "cache_results"] = False
            queries_plan.loc[:, "cache_ir"] = False
            queries_plan.loc[:, "write_inc_table"] = False

            # get affected queries
            # update deltas + mark as "dirty"
            write_tables = set(dependencies["write_table"])
            affected_queries_mask = self.cache.cache.apply(
                lambda q: len(set(q["read_tables"].split(",")) & write_tables) > 0
            )
            self.cache.cache.loc[affected_queries_mask] = True
            self.hourly_load[str(self.current_hour)] += required_capacity
            start = len(self.wl_execution_plan)
            end = start + len(queries_plan) - 1
            self.wl_execution_plan.loc[start:end] = queries_plan.values
        else:
            return False

        return True

    def execute_incrementally(self, query):
        # check if there are pending queries => execute
        qid = self.dependency_graph.add_query(query)
        dependencies = self.dependency_graph.get_all_dependencies(qid)

        if not dependencies.empty:
            queries_plan = pd.DataFrame()
            queries_plan.loc[:len(dependencies) - 1] = dependencies
            queries_plan.loc[:, "timestamp"] = query["timestamp"]
            queries_plan.loc[:, "hour"] = self.current_hour
            queries_plan.loc[:, "execution"] = "normal"
            queries_plan.loc[:, "was_cached"] = False
            queries_plan.loc[:, "cache_results"] = False
            queries_plan.loc[:, "cache_ir"] = False
            queries_plan.loc[:, "write_inc_table"] = False

            write_tables = set(dependencies["write_table"])
            affected_queries_mask = self.cache.cache.apply(
                lambda q: len(set(q["read_tables"].split(",")) & write_tables) > 0
            )
            self.cache.cache.loc[affected_queries_mask] = True

            start = len(self.wl_execution_plan)
            end = start + len(queries_plan) - 1
            self.wl_execution_plan.loc[start:end] = queries_plan.values

            self.hourly_load[str(self.current_hour)] += queries_plan["load"].sum()

        self.dependency_graph.remove_with_dependencies(qid)

        cached_query = self.cache.get(query["query_hash"])
        if cached_query["dirty"]:
            scan_delta = abs(query["bytes_scanned"] - cached_query["bytes_scanned"])
            result_delta = abs(query["result_size"] - cached_query["result_size"])
            i_result_delta = abs(query["intermediate_result_size"] - cached_query["intermediate_result_size"])

            query["bytes_scanned"] = scan_delta
            query["result_size"] = result_delta
            query["intermediate_result_size"] = i_result_delta

            query["was_cached"] = False
            query["write_inc_table"] = False
            query["cache_reads"] += 1
            query["size"] = query["result_size"] + query["intermediate_result_size"]
            is_cached = self.cache.put(query["query_hash"], query)

            if is_cached:
                query["cache_ir"] = True
                query["cache_result"] = True
                query["cache_writes"] += 1
                query["dirty"] = False

            query["execution"] = "incremental"
        else:
            query["was_cached"] = True
            query["cache_writes"] = 0
            query["bytes_scanned"] = 0
            query["cpu_time"] = 0
            query["write_volume"] = 0
            query["cache_reads"] += 1
            query["execution"] = "incremental"

        query["load"] = estimate_query_load(query, self.load_ref)
        self.hourly_load[str(self.current_hour)] += query["load"]
        self.wl_execution_plan.loc[len(self.wl_execution_plan)] = query


    def execute(self, query):
        # normal execution
        pass

    def generate_workload_execution_plan(self):
        self.wl_execution_plan = pd.DataFrame()
        for _, query in self.wl.iterrows():
            is_read = query["query_type"] == "select"
            is_write = not is_read

            h = query["hour"]

            if is_write:
                self.execute_write(query, query["timestamp"])

            if query["query_hash"] in self.cache:
                self.execute_incrementally(query)
            else:
                pass