import pandas as pd

from cache.base import CacheBase


class RepetitionAwareCache(CacheBase):
    """
    cache = {
        query_hash: {
            size: float64 # in bytes,
            repetition_coefficient: float64,
            read_tables: [],
            unique_db_instance: int64,
            ...
        }
    }
    """

    def __init__(self, max_capacity, structure, types, index_by):
        super().__init__(max_capacity)
        self.cache = pd.DataFrame(
            columns=structure
        ).astype(types)
        self.cache.set_index(index_by, inplace=True)
        self.lowest_repetition_coefficient = None


    def get_affected_queries(self, query):
        mask1 = self.cache["read_tables"].apply(lambda tables: query.write_table in tables)
        mask2 = self.cache["unique_db_instance"] == query.unique_db_instance

        return self.cache[mask1 & mask2]

    def select_query_for_eviction(self):
        return self.cache["repetition_coefficient"].idxmin()

    def evict_query(self, key):
        evicted_space = self.cache.loc[key]["size"]
        self.cache = self.cache.drop(index=key)
        self.lowest_repetition_coefficient = self.cache["repetition_coefficient"].min()
        self.usage -= evicted_space

    def evict(self, space):
        evicted_space = 0
        while evicted_space < space:
            query_hash = self.select_query_for_eviction()
            evicted_space += self.cache.loc[query_hash]["size"]
            self.cache = self.cache.drop(index=query_hash)
            self.lowest_repetition_coefficient = self.cache["repetition_coefficient"].min()
            self.insights["evictions"] += 1

        self.usage -= evicted_space

    def put(self, key, query):
        self.insights["put_requests"] += 1

        # if query already in cache => evict and re-cache

        try:
            item = self.cache.loc[key]
        except KeyError as e:
            item = None

        if item is not None:
            self.evict_query(key)

        if query["size"] < 0 or query["repetition_coefficient"] == 0 or (self.max_capacity and query["size"] > self.max_capacity):
            return False

        if not self.can_fit(query["size"]):
            if query["repetition_coefficient"] > self.lowest_repetition_coefficient:
                remaining_space = self.max_capacity - self.usage
                space = query["size"] - remaining_space
                self.evict(space)
            else:
                return False

        self.cache.loc[key] = query

        if self.lowest_repetition_coefficient is None or query["repetition_coefficient"] < self.lowest_repetition_coefficient:
            self.lowest_repetition_coefficient = query["repetition_coefficient"]

        self.usage += query["size"]

        return True
