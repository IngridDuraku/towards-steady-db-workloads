import numpy as np
import pandas as pd

from cache.repetition_aware import RepetitionAwareCache
from execution_model.models.base import BaseExecutionModel
from execution_model.utils.const import CACHE_COLS_LIST, CACHE_TYPES_DICT, WORKLOAD_PLAN_COL_LIST, ExecutionTrigger
from execution_model.utils.dependency_graph import DependencyGraph
from utils.workload import estimate_query_load


class HybridModel(BaseExecutionModel):
    def __init__(self, wl, cache_config, load_ref):
        super().__init__(wl)
        self.cache_config = cache_config
        self.cache = RepetitionAwareCache(
            max_capacity=cache_config["max_capacity"],
            structure=CACHE_COLS_LIST,
            types=CACHE_TYPES_DICT,
            index_by="query_hash"
        )
        self.dependency_graph = DependencyGraph(
            pd.DataFrame({}, columns=WORKLOAD_PLAN_COL_LIST +  ["id"])
        )
        self.load_ref = load_ref
        # self.set_execution_hour()
        self.current_hour = 1
        self.load_threshold = self.get_load_threshold()
        self.hourly_load = { str(i): 0 for i in range(1, max(self.wl["hour"]) + 1) }
        self.wl_execution_plan = pd.DataFrame(
            columns=WORKLOAD_PLAN_COL_LIST
        )

    def get_load_threshold(self):
        self.wl["load"] = self.wl.apply(lambda query: estimate_query_load(query, self.load_ref), axis=1)
        df_hr = self.wl.groupby(["hour"])["load"].sum().reset_index(name="load")
        load_threshold = df_hr["load"].mean()

        return 0.7 * load_threshold # 10% tolerance

    def run_dependencies(self, dependencies, timestamp, execution_trigger, triggered_by):
        dependencies = dependencies.drop(columns="id")
        queries_plan = dependencies.copy()
        queries_plan.loc[:, "timestamp"] = timestamp
        queries_plan.loc[:, "hour"] = self.current_hour
        queries_plan.loc[:, "execution"] = "normal"
        queries_plan.loc[:, "was_cached"] = False
        queries_plan.loc[:, "cache_result"] = False
        queries_plan.loc[:, "cache_ir"] = False
        queries_plan.loc[:, "write_inc_table"] = False
        queries_plan.loc[:, "execution_trigger"] = execution_trigger.value
        queries_plan.loc[:, "triggered_by"] = triggered_by

        # get affected queries
        # update deltas + mark as "dirty"
        write_tables = set(dependencies["write_table"])
        affected_queries_mask = self.cache.cache.apply(
            lambda q: len(set(q["read_tables"].split(",")) & write_tables) > 0,
            axis=1
        )
        self.cache.cache.loc[affected_queries_mask, "dirty"] = True
        # FIXME: results in larger deltas but that is OK for now
        self.cache.cache.loc[affected_queries_mask, "delta"] = dependencies["write_volume"].sum()

        self.wl_execution_plan = pd.concat([self.wl_execution_plan, queries_plan], ignore_index=True)
        self.hourly_load[str(self.current_hour)] += queries_plan["load"].sum()

    def execute_write(self, query,  trigger=ExecutionTrigger.IMMEDIATE, timestamp=None):
        if timestamp is None:
            timestamp = query["timestamp"]

        if not "id" in query.index:
            # add to dependency graph
            qid = self.dependency_graph.add_query(query)
        else:
            qid = query["id"]
            if self.dependency_graph.dependencies.get(qid, None) is None:
                return True # query was executed before

        dependencies = self.dependency_graph.get_all_dependencies(qid)
        # check if there is capacity
        required_capacity = query["load"] + dependencies["load"].sum()
        if self.load_threshold - self.hourly_load[str(self.current_hour)] >= required_capacity:
            if not dependencies.empty:
                self.run_dependencies(dependencies, timestamp, ExecutionTrigger.TRIGGERED_BY_WRITE ,query["query_hash"])
                query["cache_writes"] += 1  # mark affected queries

            self.dependency_graph.remove_with_dependencies(qid)

            if not self.cache.cache.empty:
                write_table = {query["write_table"]}
                affected_queries_mask = self.cache.cache.apply(
                    lambda q: len(set(q["read_tables"].split(",")) & write_table) > 0,
                    axis=1
                )
                self.cache.cache.loc[affected_queries_mask, "dirty"] = True
                self.cache.cache.loc[affected_queries_mask, "delta"] += query["write_volume"]

            query["execution"] = "normal"
            query["cache_reads"] += 1 # read dependencies
            query["execution_trigger"] = trigger.value # read dependencies
            query["triggered_by"] = query["query_hash"] # read dependencies
            query["timestamp"] = timestamp
            query["hour"] = self.current_hour

            self.hourly_load[str(self.current_hour)] += query["load"]
            self.wl_execution_plan.loc[len(self.wl_execution_plan)] = query
        else:
            return False

        return True

    def execute_incrementally(self, query, query_hash, trigger=ExecutionTrigger.IMMEDIATE, timestamp=None):
        if timestamp is None:
            timestamp = query["timestamp"]
        # check if there are pending queries => execute
        qid = self.dependency_graph.add_query(query)
        dependencies = self.dependency_graph.get_all_dependencies(qid)

        if not dependencies.empty:
            self.run_dependencies(dependencies, query["timestamp"], ExecutionTrigger.TRIGGERED_BY_READ, query_hash)

        self.dependency_graph.remove_with_dependencies(qid)

        cached_query = self.cache.get(query_hash)
        if cached_query["dirty"]:
            scan_delta = cached_query["delta"]
            result_delta = query["scan_to_result_ratio"] * scan_delta
            i_result_delta = query["scan_to_i_result_ratio"] * scan_delta

            query.loc["bytes_scanned"] = scan_delta
            query.loc["result_size"] = result_delta
            query.loc["intermediate_result_size"] = i_result_delta

            query.loc["was_cached"] = False
            query.loc["write_inc_table"] = False
            query.loc["cache_reads"] = 1
            query.loc["size"] = query["result_size"] + query["intermediate_result_size"]
            query.loc["dirty"] = False
            query.loc["delta"] = 0
            query.loc["timestamp"] = timestamp
            query.loc["hour"] = self.current_hour

            is_cached = self.cache.put(query_hash, query)

            if is_cached:
                query.loc["cache_ir"] = True
                query.loc["cache_result"] = True
                query.loc["cache_writes"] = 1
        else:
            query.loc["was_cached"] = True
            query.loc["cache_writes"] = 0
            query.loc["bytes_scanned"] = 0
            query.loc["cpu_time"] = 0
            query.loc["write_volume"] = 0
            query.loc["cache_reads"] += 1

        query.loc["execution"] = "incremental"
        query.loc["execution_trigger"] = trigger.value

        if trigger == ExecutionTrigger.IMMEDIATE:
            query["triggered_by"] = query["query_hash"]
        else:
            query["triggered_by"] = None

        query.loc["load"] = estimate_query_load(query, self.load_ref)
        self.hourly_load[str(self.current_hour)] += query["load"]
        self.wl_execution_plan.loc[len(self.wl_execution_plan)] = query

    def execute_read(self, query):
        # normal execution
        # check for pending writes
        qid = self.dependency_graph.add_query(query)
        dependencies = self.dependency_graph.get_all_dependencies(qid)

        if not dependencies.empty:
            self.run_dependencies(dependencies, query["timestamp"], ExecutionTrigger.TRIGGERED_BY_READ, query["query_hash"])

        self.dependency_graph.remove_with_dependencies(qid)

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
            query["write_inc_table"] = False
            query["cache_writes"] += 1

        query["execution"] = "normal"
        query["execution_trigger"] = ExecutionTrigger.IMMEDIATE.value
        query["triggered_by"] = query["query_hash"]
        self.hourly_load[str(self.current_hour)] += query["load"]
        self.wl_execution_plan.loc[len(self.wl_execution_plan)] = query

    def refresh_cache(self, count, timestamp):
        cache = self.cache.cache[self.cache.cache["dirty"]]
        count = min(count, len(cache))
        cache = cache.sort_values(by=["repetition_coefficient", "load"], ascending=False).iloc[:count]

        for hash_index, query in cache.iterrows():
            self.execute_incrementally(query, hash_index, ExecutionTrigger.DEFERRED, timestamp)
            if self.load_threshold - self.hourly_load[str(self.current_hour)] <= 0:
                break

    def generate_workload_execution_plan(self):
        if self.wl_execution_plan.empty:
            last_timestamp = None

            for _, query in self.wl.iterrows():
                is_read = query["query_type"] == "select"
                is_write = not is_read

                h = query["hour"]

                if h > self.current_hour:
                    # run pending writes
                    # refresh cache

                    while self.load_threshold - self.hourly_load[str(self.current_hour)] > 0:
                        # refresh cache for repetitive & expensive queries
                        self.refresh_cache(20, last_timestamp)

                        # try to execute more pending queries
                        key_pool = list(self.dependency_graph.dependencies.keys())
                        count = min(10, len(key_pool))
                        # TODO: prioritize (not randomly)
                        keys = np.random.choice(key_pool, count)
                        queries = self.dependency_graph.df[self.dependency_graph.df["id"].isin(keys)]

                        run_query = True
                        for _, q in queries.iterrows():
                            run_query = self.execute_write(q, ExecutionTrigger.DEFERRED, last_timestamp)
                            if not run_query:
                                break

                        if queries.empty or not run_query:
                            break

                    self.current_hour += 1

                last_timestamp = query["timestamp"]

                if is_write:
                    self.execute_write(query)
                    continue

                if query["query_hash"] in self.cache:
                    self.execute_incrementally(query, query["query_hash"])
                else:
                    self.execute_read(query)


        self.wl_execution_plan.loc[:, "threshold"] = self.load_threshold
        return self.wl_execution_plan
