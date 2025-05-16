import pandas as pd
from typing import Set, Dict
from collections import defaultdict

class DependencyGraph:
    def __init__(self, df: pd.DataFrame):
        self.df = df  # DataFrame containing all queries
        self.dependencies: Dict[int, Set[int]] = defaultdict(set)
        self._id_counter = 0

    def add_query(self, new_row: pd.Series):
        new_id = self._id_counter
        self._id_counter += 1

        new_row["id"] = new_id
        self.df.loc[len(self.df)] = new_row

        for _, prev_row in self.df.iterrows():
            prev_id = prev_row["id"]
            if prev_row["timestamp"] >= new_row["timestamp"]:
                continue

            # RAW: current query reads from a table previously written
            if not pd.isna(prev_row["write_table"]) and \
                new_row["unique_db_instance"] == prev_row["unique_db_instance"] and \
               len(set(new_row["read_tables"].split(",")) & {prev_row["write_table"]}) > 0:
                self.dependencies[new_id].add(prev_id)

        return new_id

    def get_all_dependencies(self, query_id: int) -> Set[int]:
        visited = set()
        def dfs(qid):
            for dep in self.dependencies.get(qid, []):
                if dep not in visited:
                    visited.add(dep)
                    dfs(dep)
        dfs(query_id)

        return visited

    def remove(self, query_id):
        if query_id not in self.df["id"].values:
            return False

        vals = self.dependencies.values()
        flat_deps = set().union(*vals)

        if query_id in flat_deps:
            return False

        all_to_remove = query_id
        self.df = self.df[~self.df["id"].isin(all_to_remove)].reset_index(drop=True)
        self.dependencies.pop(query_id)

        return True

    def remove_with_dependencies(self, query_id: int) -> bool:
        if query_id not in self.df["id"].values:
            return False

        all_deps = self.get_all_dependencies(query_id)
        all_to_remove = all_deps | {query_id}
        self.df = self.df[~self.df["id"].isin(all_to_remove)].reset_index(drop=True)

        for qid in all_to_remove:
            self.dependencies.pop(qid, None)

        for deps in self.dependencies.values():
            deps.difference_update(all_to_remove)

        return True

