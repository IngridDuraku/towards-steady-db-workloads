from pathlib import Path

import pandas as pd

from evaluation.hw_params import HW_PARAMETERS
from execution_model.models.eager import EagerExecutionModel
from execution_model.models.hybrid import HybridModel
from execution_model.models.lazy import LazyExecutionModel
from execution_model.models.one_off import OneOffExecutionModel
from utils.file import load_json
from utils.workload import estimate_query_load


class HourlyRunExperiment:
    def __init__(self, cluster_id):
        # todo: load cluster data: config, workload
        # generate execution plans for one-off, eager, lazy & hybrid
        # save plans
        path = f"data/c{cluster_id}"
        self.cluster_id = cluster_id
        self.wl = pd.read_csv(f"{path}/wl.csv")
        self.wl["timestamp"] = pd.to_datetime(self.wl["timestamp"])
        self.wl["hour"] = self.wl["hour"].astype(int)
        self.wl["write_table"] = self.wl["write_table"].fillna("")
        self.wl["read_tables"] = self.wl["read_tables"].fillna("")

        self.wl_config = load_json(f"{path}/config.json")
        self.wl_insights = load_json(f"{path}/insights.json")
        self.cache_params = HW_PARAMETERS["cache"]["s3"]
        self.cache_config = {
            "max_capacity": None,
            "cost_per_gb": self.cache_params["cost_per_gb"],
            "put_cost": self.cache_params["put_cost"],
            "get_cost": self.cache_params["get_cost"]
        }
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
        result_path = f"results/c{self.cluster_id}"
        path = Path(result_path)
        path.mkdir(parents=True, exist_ok=True)

        print("One-Off Running")
        one_off_model = OneOffExecutionModel(self.wl)
        one_off_plan = one_off_model.generate_workload_execution_plan()
        one_off_plan.to_csv(f"{result_path}/one_off_plan.csv")

        print("Eager Running")
        eager_model = EagerExecutionModel(self.wl, self.cache_config)
        eager_plan = eager_model.generate_workload_execution_plan()
        eager_plan["load"] =  eager_plan.apply(lambda q: estimate_query_load(q, self.load_ref), axis=1)
        eager_plan.to_csv(f"{result_path}/eager_plan.csv")

        print("Lazy Running")
        lazy_model = LazyExecutionModel(self.wl, self.cache_config)
        lazy_plan = lazy_model.generate_workload_execution_plan()
        lazy_plan["load"] =  lazy_plan.apply(lambda q: estimate_query_load(q, self.load_ref), axis=1)
        lazy_plan.to_csv(f"{result_path}/lazy_plan.csv")

        print("Hybrid Running")
        hybrid_model = HybridModel(self.wl, self.cache_config, self.load_ref)
        hybrid_plan = hybrid_model.generate_workload_execution_plan()
        hybrid_plan["load"] =  hybrid_plan.apply(lambda q: estimate_query_load(q, self.load_ref), axis=1)
        hybrid_plan.to_csv(f"{result_path}/hybrid_plan.csv")


if __name__ == "__main__":
    clusters = [14]
    for cluster_id in  clusters:
        print("Cluster ID: ", cluster_id)
        experiment = HourlyRunExperiment(cluster_id)
        experiment.run()