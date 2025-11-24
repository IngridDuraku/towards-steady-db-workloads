from pathlib import Path

import pandas as pd

from evaluation.hw_params import HW_PARAMETERS
from evaluation.utils import estimate_latency
from execution_model.models.hybrid import HybridModel
from utils.file import load_json


class CacheTypeComparison:
    def __init__(self):
        self.wls = [
            "wl1",
             "wl2",
             "wl3"
             "wl4"
        ]
        self.cache_size_map = {
            "wl1": 8,
            "wl2": 64,
            "wl3": 256,
            "wl4": 8
        }
        self.name = "exp_5"

    def run(self):
        executions = []
        cache_types = ["s3", "gp3"]
        result_path = f"results/{self.name}"
        path = Path(result_path)
        path.mkdir(parents=True, exist_ok=True)

        for wl_name in self.wls:
            wl = pd.read_csv(f"data/{wl_name}/wl.csv")
            wl["hour"] = wl["hour"].astype(int)
            wl_config = load_json(f"data/{wl_name}/config.json")
            cache_size = self.cache_size_map[wl_name]
            load_ref = {
                "bytes_scanned": (wl_config["query_config"]["bytes_scanned"]["lower_bound_mb"] * 1e6 +
                                  wl_config["query_config"]["bytes_scanned"][
                                      "upper_bound_gb"] * 1e9) / 2,
                "result_size": (wl_config["query_config"]["result_size"]["lower_bound_mb"] * 1e6 +
                                wl_config["query_config"]["result_size"][
                                    "upper_bound_gb"] * 1e9) / 2,
                "write_volume": (wl_config["query_config"]["write_volume"]["lower_bound_mb"] * 1e6 +
                                 wl_config["query_config"]["write_volume"][
                                     "upper_bound_gb"] * 1e9) / 2,
                "cpu_time": wl["cpu_time"].median(),
            }

            for cache_type in cache_types:
                print(f"Workload: {wl_name}, cache_type: {cache_type}")
                cache_params = HW_PARAMETERS["cache"][cache_type]
                hw_params = {
                    "instance": HW_PARAMETERS["aws_instances"]["c5n.large"],
                    "cache": cache_params
                }
                cache_config = {
                    "max_capacity": cache_size * 1e9,
                    "cost_per_gb": cache_params["cost_per_gb"],
                    "put_cost": cache_params["put_cost"],
                    "get_cost": cache_params["get_cost"],
                    "cache_type": "gp3"
                }
                model = HybridModel(wl, cache_config, load_ref)
                plan = model.generate_workload_execution_plan()
                plan["latency"] = estimate_latency(plan, hw_params)

                plan.to_csv(f"{result_path}/plan_{wl_name}_{cache_type}.csv")

                storage_cost = model.get_storage_cost(hw_params)
                compute_cost = model.get_compute_cost(hw_params)
                total_cost = compute_cost + storage_cost

                row = {
                    "wl": wl_name,
                    "cache_size": cache_size,
                    "cache_type": cache_type,
                    "model": "hybrid",
                    "storage_cost": storage_cost,
                    "compute_cost": compute_cost,
                    "total_cost": total_cost,
                    "result_size": plan["result_size"].mean()
                }
                print(row)

                executions.append(row)

        result = pd.DataFrame(data=executions)
        result.to_csv(f"{result_path}/result.csv")

if __name__ == "__main__":
    experiment = CacheTypeComparison()
    experiment.run()