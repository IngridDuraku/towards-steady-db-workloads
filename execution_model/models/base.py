from abc import ABC, abstractmethod

from pricing_calculator.basic_runtime_estimator import BasicRuntimeEstimator
from pricing_calculator.pricing_calculator import PricingCalculator


class BaseExecutionModel(ABC):
    def __init__(self, wl):
        self.wl = wl
        # add new columns with default values
        self.wl["cache_result"] = False
        self.wl["cache_ir"] = False
        self.wl["write_delta"] = False
        self.wl["was_cached"] = False

        self.wl["cache_writes"] = 0
        self.wl["cache_reads"] = 0

        self.wl_execution_plan = None
        self.cache = None

    @abstractmethod
    def generate_workload_execution_plan(self):
        pass

    def get_runtime(self, hw_parameters):
        if self.wl_execution_plan is None:
            self.generate_workload_execution_plan()

        return BasicRuntimeEstimator.get_wl_total_runtime(hw_parameters, self.wl_execution_plan)

    def get_cost(self, hw_parameters):
        if self.wl_execution_plan is None:
            self.generate_workload_execution_plan()

        cache_usage = 0
        if self.cache:
            cache_usage = self.cache.usage

        return PricingCalculator.get_total_cost(hw_parameters, self.wl_execution_plan, cache_usage)

    def get_compute_cost(self, hw_parameters):
        if self.wl_execution_plan is None:
            self.generate_workload_execution_plan()

        return PricingCalculator.get_compute_cost(hw_parameters, self.wl_execution_plan)

    def get_storage_cost(self, hw_parameters):
        if self.wl_execution_plan is None:
            self.generate_workload_execution_plan()

        cache_usage = 0
        if self.cache:
            cache_usage = self.cache.usage

        return PricingCalculator.get_storage_cost(hw_parameters, self.wl_execution_plan, cache_usage)
