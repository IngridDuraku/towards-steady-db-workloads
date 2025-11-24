from pathlib import Path

import pandas as pd

from evaluation.hw_params import HW_PARAMETERS
from execution_model.models.eager import EagerExecutionModel
from execution_model.models.hybrid import HybridModel
from execution_model.models.lazy import LazyExecutionModel
from execution_model.models.one_off import OneOffExecutionModel
from execution_model.utils.const import WORKLOAD_TYPES_DICT
from utils.file import load_json


class CostComparisonExperiment:
    def __init__(self, wl, wl_config):
        self.wl = wl
        self.wl_config = wl_config

        self.config = load_json("config.json")
        self.cache_params = HW_PARAMETERS["cache"][self.config["cache_type"]]
        self.cache_config = {
            "max_capacity": None, # variable
            "cost_per_gb": self.cache_params["cost_per_gb"],
            "put_cost": self.cache_params["put_cost"],
            "get_cost": self.cache_params["get_cost"],
            "cache_type": "gp3"
        }
        self.name = self.config["name"]
        self.load_ref = {
            "bytes_scanned": (self.wl_config["query_config"]["bytes_scanned"]["lower_bound_mb"] * 1e6 +
                              self.wl_config["query_config"]["bytes_scanned"][
                                  "upper_bound_gb"] * 1e9) / 2,
            "result_size": (self.wl_config["query_config"]["result_size"]["lower_bound_mb"] * 1e6 +
                            self.wl_config["query_config"]["result_size"][
                                "upper_bound_gb"] * 1e9) / 2,
            "write_volume": (self.wl_config["query_config"]["write_volume"]["lower_bound_mb"] * 1e6 +
                             self.wl_config["query_config"]["write_volume"][
                                 "upper_bound_gb"] * 1e9) / 2,
            "cpu_time": self.wl["cpu_time"].median(),
        }


    def run(self):
        result_path = f"results/{self.name}_with_pending"
        path = Path(result_path)
        path.mkdir(parents=True, exist_ok=True)

        hw_params = {
            "instance": HW_PARAMETERS["aws_instances"]["c5n.large"],
            "cache": self.cache_params
        }

        one_off_model = OneOffExecutionModel(self.wl)
        one_off_plan = one_off_model.generate_workload_execution_plan()
        one_off_plan.to_csv(f"{result_path}/one_off_plan.csv")
        one_off_comp_cost = one_off_model.get_compute_cost(hw_params)
        one_off_sto_cost = one_off_model.get_storage_cost(hw_params)
        one_off_pending_cost = one_off_model.get_pending_cost(hw_params)
        one_off_cost = one_off_model.get_cost(hw_params)

        cache_sizes = self.config["cache_size_range_gb"]
        data = []

        for size in cache_sizes:
            print(f"Estimating cost for cache size {size}")
            self.cache_config["max_capacity"] = size * 1e9

            one_off_row = {
                "model": "one-off",
                "total_cost": one_off_cost,
                "compute": one_off_comp_cost,
                "pending": one_off_pending_cost,
                "storage": one_off_sto_cost,
                "cache_size": size
            }
            data.append(one_off_row)

            # hybrid
            print("Hybrid Running")
            hybrid_model = HybridModel(self.wl, self.cache_config, self.load_ref)
            hybrid_plan = hybrid_model.generate_workload_execution_plan()
            hybrid_plan.to_csv(f"{result_path}/hybrid_plan_{size}.csv")
            comp_cost = hybrid_model.get_compute_cost(hw_params)
            sto_cost = hybrid_model.get_storage_cost(hw_params)
            pending_cost = hybrid_model.get_pending_cost(hw_params)
            cost = hybrid_model.get_cost(hw_params)

            hybrid_row = {
                "model": "hybrid",
                "total_cost": cost,
                "compute": comp_cost,
                "storage": sto_cost,
                "pending": pending_cost,
                "cache_size": size
            }

            data.append(hybrid_row)

            # eager
            print("Eager Running")
            eager_model = EagerExecutionModel(self.wl, self.cache_config)
            eager_plan = eager_model.generate_workload_execution_plan()
            eager_plan.to_csv(f"{result_path}/eager_plan_{size}.csv")
            comp_cost = eager_model.get_compute_cost(hw_params)
            sto_cost = eager_model.get_storage_cost(hw_params)
            pending_cost = eager_model.get_pending_cost(hw_params)
            cost = eager_model.get_cost(hw_params)

            eager_row = {
                "model": "eager",
                "total_cost": cost,
                "compute": comp_cost,
                "storage": sto_cost,
                "pending": pending_cost,
                "cache_size": size
            }

            data.append(eager_row)

            # lazy
            print("Lazy Running")
            lazy_model = LazyExecutionModel(self.wl, self.cache_config)
            lazy_plan = lazy_model.generate_workload_execution_plan()
            lazy_plan.to_csv(f"{result_path}/lazy_plan_{size}.csv")
            comp_cost = lazy_model.get_compute_cost(hw_params)
            sto_cost = lazy_model.get_storage_cost(hw_params)
            pending_cost = lazy_model.get_pending_cost(hw_params)
            cost = lazy_model.get_cost(hw_params)

            lazy_row = {
                "model": "lazy",
                "total_cost": cost,
                "compute": comp_cost,
                "storage": sto_cost,
                "pending": pending_cost,
                "cache_size": size
            }

            data.append(lazy_row)

        cost_df = pd.DataFrame(data=data, columns=[
            "model",
            "compute",
            "storage",
            "pending",
            "cache_size",
            "total_cost",
        ])

        cost_df.to_csv(f"{result_path}/results.csv")


if __name__ == "__main__":
    path = "data/wl1"
    wl = pd.read_csv(f"{path}/wl.csv").astype(WORKLOAD_TYPES_DICT)
    wl_config = load_json(f"{path}/config.json")
    experiment = CostComparisonExperiment(wl, wl_config)
    experiment.run()