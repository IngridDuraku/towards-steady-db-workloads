from pricing_calculator.basic_runtime_estimator import BasicRuntimeEstimator


class PricingCalculator:
    @staticmethod
    def get_total_cost(hw_parameters, wl, cache_usage):
        total_runtime = BasicRuntimeEstimator.get_wl_total_runtime(hw_parameters, wl)
        runtime_cost = total_runtime * hw_parameters["instance"]["price_per_hour"] / 3600

        cache_cost = cache_usage * hw_parameters["cache"]["cost_per_gb"] / 1e9

        if hw_parameters["cache"]["type"] == "s3":
            s3_put_requests_cost = wl["cache_writes"].sum() * hw_parameters["cache"]["put_cost"] / 1000
            s3_get_requests_cost = wl["cache_reads"].sum() * hw_parameters["cache"]["put_cost"] / 1000
            cache_cost += s3_put_requests_cost + s3_get_requests_cost


        return runtime_cost + cache_cost