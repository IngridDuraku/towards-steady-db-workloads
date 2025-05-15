from abc import ABC, abstractmethod

import pandas as pd


class CacheBase(ABC):
    def __init__(self, max_capacity):
        self.max_capacity = max_capacity
        self.usage = 0
        self.cache = pd.DataFrame()
        self.insights = {
            "cache_misses": 0,
            "cache_hits": 0,
            "get_requests": 0,
            "put_requests": 0,
            "evictions": 0,
        }

    def __contains__(self, key):
        return key in self.cache.index

    def can_fit(self, value):
        if self.max_capacity is None:
            return True

        return self.max_capacity - self.usage >= value

    def get(self, key):
        self.insights["get_requests"] += 1

        try:
            item = self.cache.loc[key]
        except KeyError as e:
            item = None

        if item is None:
            self.insights["cache_misses"] += 1
        else:
            self.insights["cache_hits"] += 1

        return item

    def update_field(self, key, col, value):
        self.cache.at[key, col] = value

    def reset(self):
        self.cache = pd.DataFrame(columns=self.cache.columns)
        self.usage = 0
        self.insights = {
            "cache_misses": 0,
            "cache_hits": 0,
            "get_requests": 0,
            "put_requests": 0,
            "evictions": 0,
        }

    @abstractmethod
    def evict(self, space):
        pass

    @abstractmethod
    def put(self, key, item):
        pass
