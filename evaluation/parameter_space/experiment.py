from pathlib import Path

import pandas as pd

from evaluation.hw_params import HW_PARAMETERS
from execution_model.models.eager import EagerExecutionModel
from execution_model.models.hybrid import HybridModel
from execution_model.models.lazy import LazyExecutionModel
from execution_model.models.one_off import OneOffExecutionModel
from pricing_calculator.basic_runtime_estimator import BasicRuntimeEstimator
from utils.file import load_json, save_json_file
from workload_analyzer.workload_insights import WorkloadInsights
from workload_generator.generator import WorkloadGenerator


class ParameterSpaceExperiment:
    def __init__(self):
        config_path = "config.json"
        self.config = load_json(config_path)
        self.cache_params = HW_PARAMETERS["cache"]["gp3"]
        self.cache_config = {
            "max_capacity": 256e9,
            "cost_per_gb": self.cache_params["cost_per_gb"],
            "put_cost": self.cache_params["put_cost"],
            "get_cost": self.cache_params["get_cost"],
            "cache_type": "gp3"
        }
        self.hw_params = {
            "instance": HW_PARAMETERS["aws_instances"]["c5n.large"],
            "cache": self.cache_params
        }


    def run(self):
        pass

    def generate_workloads(self):
        bytes_scanned = [
            [
                600,  # mb
                250  # gb
            ],
            [
                600,  # mb
                250  # gb
            ],
            [
                600,  # mb
                250  # gb
            ],
        ]

        # ratio (write_volume / bytes_scanned): [ ~1, 0.5, 0.01 ]

        write_volume = [
            [
                1,
                0.5
            ],
            [
                1,  # 1 MB
                0.07  # 70 MB
            ],
            [
                0.1, # 10 - 100 KB
                0.001 # 0.1  - 1MB
            ],
        ]

        cache_sizes = [
            1e9,
            1e9,
            1e9
        ]

        name1 = [
            "high",
            "medium",
            "low"
        ]
        repetitiveness = [
            0.2, 0.4,
            0.6,
            0.8,
            0.99
        ]
        name2 = [
            "low1", "low2",
            "medium",
            "high",
            "very_high"
        ]
        write_frequency = [
            0.01, 0.1,
            0.3,
            0.5,
            0.8
        ]
        name3 = [
            "low1", "low2",
            "medium",
            "high",
            "very_high"
        ]

        config = self.config
        result = []

        for scan_size, write_size, cache, n1 in zip(bytes_scanned, write_volume, cache_sizes, name1):
            config["query_config"]["bytes_scanned"] = {
                "lower_bound_mb": scan_size[0],
                "upper_bound_gb": scan_size[1]
            }
            config["query_config"]["write_volume"] = {
                "lower_bound_mb": write_size[0],
                "upper_bound_gb": write_size[1]
            }
            self.cache_config["max_capacity"] = cache

            for r, n2 in zip(repetitiveness, name2):
                config["repetitiveness"] = r
                for wf, n3 in zip(write_frequency, name3):
                    print(f"Generating workload: workload_{n1}_{n2}_{n3}")
                    config["query_config"]["query_type_p"] = {
                        "select": 1 - wf,
                        "insert": wf,
                        "delete": 0,
                        "update": 0
                    }

                    result_path = f"data/redset_scan_3/workload_{n1}_{n2}_{n3}"
                    path = Path(result_path)
                    path.mkdir(parents=True, exist_ok=True)

                    wl_generator = WorkloadGenerator(config)
                    wl = wl_generator.generate_workload()
                    insights = WorkloadInsights(wl).get_insights()

                    wl.to_csv(f"{result_path}/wl.csv", index=False)
                    save_json_file(insights, f"{result_path}/insights.json")
                    save_json_file(config, f"{result_path}/config.json")
                    save_json_file(self.hw_params, f"{result_path}/hw_params.json")
                    save_json_file(self.cache_config, f"{result_path}/cache_config.json")

                    one_off = OneOffExecutionModel(wl)
                    one_off_plan = one_off.generate_workload_execution_plan()
                    one_off_plan.to_csv(f"{result_path}/one_off_plan.csv")
                    one_off_runtime = one_off.get_runtime(hw_parameters=self.hw_params)
                    one_off_cost = one_off.get_cost(hw_parameters=self.hw_params)
                    one_off_plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters=self.hw_params,
                                                                                               wl=one_off_plan)

                    load_ref = {
                        "bytes_scanned": (config["query_config"]["bytes_scanned"]["lower_bound_mb"] * 1e6 +
                                          config["query_config"]["bytes_scanned"][
                                              "upper_bound_gb"] * 1e9) / 2,
                        "result_size": (config["query_config"]["result_size"]["lower_bound_mb"] * 1e6 +
                                        config["query_config"]["result_size"][
                                            "upper_bound_gb"] * 1e9) / 2,
                        "write_volume": (config["query_config"]["write_volume"]["lower_bound_mb"] * 1e6 +
                                         config["query_config"]["write_volume"][
                                             "upper_bound_gb"] * 1e9) / 2,
                        "cpu_time": wl["cpu_time"].median(),
                    }

                    print("Hybrid Running")
                    hybrid_model = HybridModel(wl, self.cache_config, load_ref)
                    hybrid_plan = hybrid_model.generate_workload_execution_plan()
                    hybrid_plan.to_csv(f"{result_path}/hybrid_plan.csv")
                    pending_cost = hybrid_model.get_pending_cost(self.hw_params)
                    hybrid_cost = hybrid_model.get_cost(self.hw_params)
                    hybrid_runtime = hybrid_model.get_runtime(hw_parameters=self.hw_params)
                    hybrid_plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters=self.hw_params,
                                                                                               wl=hybrid_plan)
                    hybrid_speedup = (one_off_runtime - hybrid_runtime) / one_off_runtime
                    hybrid_cost_reduction = (one_off_cost - hybrid_cost) / one_off_cost

                    print("Lazy Running")
                    lazy_model = LazyExecutionModel(wl, self.cache_config)
                    lazy_plan = lazy_model.generate_workload_execution_plan()
                    lazy_plan.to_csv(f"{result_path}/lazy_plan.csv")
                    pending_cost = lazy_model.get_pending_cost(self.hw_params)
                    lazy_cost = lazy_model.get_cost(self.hw_params)
                    lazy_runtime = lazy_model.get_runtime(hw_parameters=self.hw_params)
                    lazy_plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters=self.hw_params,
                                                                                               wl=lazy_plan)
                    lazy_speedup = (one_off_runtime - lazy_runtime) / one_off_runtime
                    lazy_cost_reduction = (one_off_cost - lazy_cost) / one_off_cost

                    print("Eager Running")
                    eager_model = EagerExecutionModel(wl, self.cache_config)
                    eager_plan = eager_model.generate_workload_execution_plan()
                    eager_plan.to_csv(f"{result_path}/eager_plan.csv")
                    eager_cost = eager_model.get_cost(self.hw_params)
                    eager_runtime = eager_model.get_runtime(hw_parameters=self.hw_params)
                    eager_plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters=self.hw_params,
                                                                                               wl=eager_plan)

                    eager_speedup = (one_off_runtime - eager_runtime) / one_off_runtime
                    eager_cost_reduction = (one_off_cost - eager_cost) / one_off_cost

                    is_read = wl["query_type"] == "select"
                    is_write = wl["query_type"] != "select"

                    ratio = round(wl[is_write]["write_volume"].median() / wl[is_read]["bytes_scanned"].median(), 9)

                    item = {
                        "ratio_cat": n1,
                        "ratio": ratio,
                        "bytes_scanned": wl["bytes_scanned"].median(),
                        "write_volume": wl["write_volume"].median(),
                        "write_frequency": wf,
                        "repetitiveness": r,
                        "speedup": (hybrid_speedup + lazy_speedup + eager_speedup) / 3,
                        "cost_reduction": (hybrid_cost_reduction + lazy_cost_reduction + eager_cost_reduction) / 3,
                    }
                    print(item)
                    result.append(item)

        result_df = pd.DataFrame(result)
        result_df.to_csv(f"results/redset_scan_3.csv", index=False)

if __name__ == "__main__":
    experiment = ParameterSpaceExperiment()
    experiment.generate_workloads()