import pandas as pd

from execution_model.utils.const import ExecutionTrigger
from pricing_calculator.basic_runtime_estimator import BasicRuntimeEstimator


class PricingCalculator:
    @staticmethod
    def get_total_cost(hw_parameters, wl, cache_usage):
        runtime_cost = PricingCalculator.get_compute_cost(hw_parameters, wl)
        cache_cost = PricingCalculator.get_storage_cost(hw_parameters, wl, cache_usage)

        return runtime_cost + cache_cost

    @staticmethod
    def get_compute_cost(hw_parameters, wl):
        total_runtime = BasicRuntimeEstimator.get_wl_total_runtime(hw_parameters, wl)
        runtime_cost = total_runtime * hw_parameters["instance"]["price_per_hour"] / 3600

        return runtime_cost

    @staticmethod
    def get_storage_cost(hw_parameters, wl, cache_usage):
        wl["timestamp"] = pd.to_datetime(wl["timestamp"])
        duration_seconds = (wl["timestamp"].max() - wl["timestamp"].min()).total_seconds()
        month_in_seconds = 30 * 24 * 60 * 60
        cache_cost = cache_usage * hw_parameters["cache"]["cost_per_gb"] / 1e9 * duration_seconds / month_in_seconds

        if hw_parameters["cache"]["type"] == "s3":
            s3_put_requests_cost = wl["cache_writes"].sum() * hw_parameters["cache"]["put_cost"] / 1000
            s3_get_requests_cost = wl["cache_reads"].sum() * hw_parameters["cache"]["get_cost"] / 1000
            cache_cost += s3_put_requests_cost + s3_get_requests_cost

        return cache_cost

    @staticmethod
    def get_pending_cost(hw_parameters, wl):
        pending = wl["execution_trigger"] == ExecutionTrigger.PENDING.value
        queries = wl[pending]

        if queries.empty:
            return 0

        return PricingCalculator.get_compute_cost(hw_parameters, queries)
