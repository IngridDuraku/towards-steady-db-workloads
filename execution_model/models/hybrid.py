import numpy as np
import pandas as pd

from cache.repetition_aware import RepetitionAwareCache
from execution_model.models.base import BaseExecutionModel
from execution_model.utils.dependency_graph import DependencyGraph
from utils.workload import estimate_query_load


class HybridExecutionModel(BaseExecutionModel):
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
                # "runtime": "float64",
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

    def get_load_threshold(self):
        self.wl["load"] = self.wl.apply(lambda query: estimate_query_load(query, self.load_ref), axis=1)
        df_hr = self.wl.groupby(["hour"])["load"].sum().reset_index(name="load")
        load_threshold = df_hr["load"].quantile(0.5)

        return 0.7 * load_threshold # 10% tolerance

    def get_affected_queries_plan(self, write_query):
        delta = write_query["write_volume"]
        affected_queries = self.cache.get_affected_queries(write_query)

        if affected_queries.empty:
            return affected_queries

        affected_queries["bytes_scanned"] = affected_queries["delta"] +  delta
        affected_queries["result_size"] = affected_queries["scan_to_result_ratio"] * affected_queries["bytes_scanned"]
        affected_queries["intermediate_result_size"] = affected_queries[
                                                               "scan_to_i_result_ratio"] * affected_queries["bytes_scanned"]

        affected_queries["timestamp"] = write_query.timestamp
        affected_queries["hour"] = write_query.hour
        affected_queries["was_cached"] = False
        affected_queries["cache_result"] = True
        affected_queries["cache_ir"] = True
        affected_queries["write_inc_table"] = False
        affected_queries["cache_writes"] += 1
        affected_queries["execution"] = "incremental"

        affected_queries["load"] = affected_queries.apply(
            lambda query: estimate_query_load(query, self.load_ref), axis=1
        )

        return affected_queries

    # def set_execution_hour(self):
    #     start_time = self.wl["timestamp"].min()
    #     self.wl["hour"] = ((self.wl["timestamp"] - start_time).dt.total_seconds() // 3600 + 1).astype(int)

    def refresh_cache(self, count):
        mask = self.cache.cache["dirty"]
        queries = self.cache.cache[mask]
        count = min(count, len(queries))

        if count == 0:
            return queries

        queries = queries.head(count)

        queries["bytes_scanned"] = queries["delta"]
        queries["result_size"] = queries["scan_to_result_ratio"] * queries["bytes_scanned"]
        queries["intermediate_result_size"] = queries["scan_to_i_result_ratio"] * queries["bytes_scanned"]

        queries["was_cached"] = False
        queries["cache_result"] = True
        queries["cache_ir"] = True
        queries["write_inc_table"] = False
        queries["cache_reads"] += 1  # retrieve last_occ

        queries["size"] = queries["result_size"] + queries["intermediate_result_size"]
        queries["execution"] = "incremental"

        queries.apply(lambda q: self.cache.put(q.name, q), axis=1)
        queries["load"] = queries.apply(lambda q: estimate_query_load(q, self.load_ref ), axis=1)
        self.hourly_load[str(self.current_hour)] += queries["load"].sum()

        return queries

    def get_affected_queries_mask(self, query):
        mask1 = self.cache.cache["unique_db_instance"] == query["unique_db_instance"]
        mask2 = self.cache.cache["read_tables"].apply(lambda tables: query["write_table"] in tables)

        return mask1 & mask2

    def schedule_pending_writes(self, count):
        count = min(len(self.dependency_graph.dependencies), count)
        keys_to_drop = np.random.choice(list(self.dependency_graph.dependencies.keys()), count)
        queries = pd.DataFrame()

        for key in keys_to_drop:
            deps = self.dependency_graph.get_all_dependencies(key)
            deps.add(key)
            q = self.dependency_graph.df[self.dependency_graph.df["id"].isin(deps)]
            queries = pd.concat([queries, q])
            self.dependency_graph.remove_with_dependencies(key)

        return queries


    def generate_workload_execution_plan(self):
        if self.wl_execution_plan is None:
            ex_plan = []
            last_timestamp = None

            for _, query in self.wl.iterrows():
                is_read = query["query_type"] == "select"
                is_write = not is_read

                h = query["hour"]
                if h > self.current_hour:
                    # check if there is still capacity in the current hour to schedule pending writes

                    while self.load_threshold - self.hourly_load[str(self.current_hour)] > 0:
                        # todo:
                        # refresh "dirty" cache entries
                        queries = self.refresh_cache(10)
                        queries["timestamp"] = last_timestamp
                        queries["hour"] = self.current_hour

                        rows = [row for index, row in queries.iterrows()]
                        ex_plan.extend(rows)

                        # execute pending updates
                        pending_writes = self.schedule_pending_writes(2)
                        if not pending_writes.empty:
                            pending_writes["timestamp"] = last_timestamp
                            pending_writes["hour"] = self.current_hour
                            pending_writes["execution"] = "normal"
                            pending_writes["was_cached"] = False
                            pending_writes["cache_result"] = False
                            pending_writes["cache_ir"] = False
                            pending_writes["write_inc_table"] = False
                            self.hourly_load[str(self.current_hour)] += pending_writes["load"].sum()

                            rows = [row for index, row in pending_writes.iterrows()]
                            ex_plan.extend(rows)

                        if queries.empty and pending_writes.empty:
                            break

                    self.current_hour += 1

                last_timestamp = query["timestamp"]

                if is_write:
                    # strategy:
                    #  1) get dependencies
                    #  2) check if there is capacity to execute dependencies + this query
                    #  3) refresh affected queries (incremental run) - based on query runtime estimation
                    qid = self.dependency_graph.add_query(query)
                    dependencies = self.dependency_graph.get_all_dependencies(qid)
                    dependencies = self.dependency_graph.df[self.dependency_graph.df["id"].isin(dependencies)]
                    dependencies = dependencies.sort_values(by=["timestamp"])

                    execution_load = query["load"]

                    if not dependencies.empty:
                        execution_load += dependencies["load"].sum()

                    try:
                        if execution_load + self.hourly_load[str(h)] < self.load_threshold:
                            # run dependencies and current query
                            dependencies["timestamp"] = query["timestamp"]
                            dependencies["execution"] = "normal"
                            dependencies["was_cached"] = False
                            dependencies["cache_result"] = False
                            dependencies["cache_ir"] = False
                            dependencies["write_inc_table"] = False
                            dependencies["execution"] = "normal"

                            rows = [row for index, row in dependencies.iterrows()]
                            ex_plan.extend(rows)

                            query["execution"] = "normal"
                            query["cache_reads"] += 1 # for reading affected queries
                            query["cache_writes"] += 1 # for updating affected queries
                            ex_plan.append(query)
                            self.dependency_graph.remove_with_dependencies(qid)

                            self.hourly_load[str(h)] += execution_load
                        else:
                            # pend query - already added to dep graph => continue
                            continue
                    except:
                        print(query)


                    # try to refresh cache
                    affected_queries_plan = self.get_affected_queries_plan(query)

                    refresh_load = 0 if affected_queries_plan.empty else affected_queries_plan["load"].sum()

                    if refresh_load + self.hourly_load[str(h)] < self.load_threshold:
                        # add execution plan
                        rows = [row for index, row in affected_queries_plan.iterrows()]
                        ex_plan.extend(rows)
                        self.hourly_load[str(h)] += refresh_load
                    else:
                        # mark affected queries and update deltas
                        condition = self.get_affected_queries_mask(query)
                        self.cache.cache.loc[condition, "delta"] += query["write_volume"]
                        self.cache.cache.loc[condition, "dirty"] = True

                    continue

                if query["query_hash"] in self.cache:
                    cached_q = self.cache.get(query["query_hash"])
                    if cached_q["dirty"]:
                        # execute incrementally
                        scan_delta = cached_q["delta"]
                        result_delta = cached_q["scan_to_result_ratio"] * scan_delta
                        i_result_delta = cached_q["scan_to_i_result_ratio"] * scan_delta

                        query["bytes_scanned"] = scan_delta
                        query["result_size"] = result_delta
                        query["intermediate_result_size"] = i_result_delta
                        query["cache_reads"] += 1  # retrieve last_occ

                        re_cached_query = query
                        re_cached_query["size"] = query["result_size"] + query["intermediate_result_size"]
                        re_cached_query["dirty"] = False
                        re_cached_query["delta"] = 0

                        is_cached = self.cache.put(query.query_hash, re_cached_query)

                        if is_cached:
                            query["cache_ir"] = True
                            query["cache_result"] = True
                            query["cache_writes"] += 1

                        query["execution"] = "incremental"
                        query["load"] = estimate_query_load(query, self.load_ref)
                        self.hourly_load[str(h)] += query["load"]
                        ex_plan.append(query)
                    else:
                        # read result from cache
                        query["was_cached"] = True
                        query["cache_writes"] = 0
                        query["bytes_scanned"] = 0
                        query["cpu_time"] = 0
                        query["write_volume"] = 0
                        query["cache_reads"] += 1
                        query["execution"] = "incremental"
                        query["load"] = estimate_query_load(query, self.load_ref)
                        self.hourly_load[str(h)] += query["load"]

                        ex_plan.append(query)
                        continue
                else:
                    # execute from scratch
                    # run all pending queries
                    # remove them from dep graph
                    # run query
                    qid = self.dependency_graph.add_query(query)
                    deps = self.dependency_graph.get_all_dependencies(qid)
                    pending_updates = self.dependency_graph.df[self.dependency_graph.df["id"].isin(deps)]
                    pending_updates = pending_updates.sort_values(by=["timestamp"])

                    pending_updates["timestamp"] = query["timestamp"]
                    pending_updates["hour"] = query["hour"]
                    pending_updates["execution"] = "normal"
                    pending_updates["was_cached"] = False
                    pending_updates["cache_result"] = False
                    pending_updates["cache_ir"] = False
                    pending_updates["write_inc_table"] = False

                    rows = [row for index, row in pending_updates.iterrows()]
                    ex_plan.extend(rows)

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
                    execution_load = query["load"]

                    if not pending_updates.empty:
                        execution_load += pending_updates["load"].sum()

                    self.hourly_load[str(h)] += execution_load
                    ex_plan.append(query)

            self.wl_execution_plan = pd.DataFrame(data=ex_plan)

        return self.wl_execution_plan
