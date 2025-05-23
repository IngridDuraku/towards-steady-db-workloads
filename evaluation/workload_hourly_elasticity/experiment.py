import pandas as pd
from plotnine import ggplot, aes, geom_bar, labs, scale_y_continuous, ylim, theme_bw, scale_fill_manual, theme, \
    element_text, element_blank

from execution_model.models.eager import EagerExecutionModel
from execution_model.models.hybrid import HybridExecutionModel
from execution_model.models.lazy import LazyExecutionModel
from execution_model.models.one_off import OneOffExecutionModel
from tests.main import HW_PARAMETERS
from utils.file import load_json
from utils.workload import estimate_query_load
from workload_analyzer.redset_workload_extractor import RedsetWorkloadExtractor
from workload_generator.generator import WorkloadGenerator

class WorkloadHourlyElasticityExperiment:
    def __init__(self, base_config):
        self.base_config = base_config
        self.cache_params = HW_PARAMETERS["cache"][base_config["cache_type"]]
        self.cache_config = {
            "max_capacity": base_config["max_cache_capacity"],
            "cost_per_gb": self.cache_params["cost_per_gb"],
            "put_cost": self.cache_params["put_cost"],
            "get_cost": self.cache_params["get_cost"]
        }
        extractor = RedsetWorkloadExtractor(
            cluster_id=base_config["cluster_id"],
        )
        config = extractor.export_config(base_config)
        # config["size"] = 2000
        wl_generator = WorkloadGenerator(config)
        self.wl = wl_generator.generate_workload()
        self.wl["hour"] = self.wl["hour"].astype(int)
        # self.wl = pd.read_csv("data/wl_hourly.csv")
        # self.wl["timestamp"] = pd.to_datetime(self.wl["timestamp"])
        self.load_ref = {
            "bytes_scanned": (config["query_config"]["bytes_scanned"]["lower_bound_mb"] * 1e6 + config["query_config"]["bytes_scanned"][
                "upper_bound_gb"] * 1e9) / 2,
            "result_size": (config["query_config"]["result_size"]["lower_bound_mb"] * 1e6 + config["query_config"]["result_size"][
                "upper_bound_gb"] * 1e9) / 2,
            "write_volume": (config["query_config"]["write_volume"]["lower_bound_mb"] * 1e6 + config["query_config"]["write_volume"][
                "upper_bound_gb"] * 1e9) / 2,
            "cpu_time": self.wl["cpu_time"].median(),
        }

    def get_hourly(self, wl):
        is_read = wl["query_type"] == "select"
        is_write = wl["query_type"] != "select"
        wl.loc[is_read, "type"] = "Read-Only"
        wl.loc[is_write, "type"] = "Write"

        df_hourly = wl.groupby(["hour", "type"])["load"].sum().reset_index(name="load")

        return df_hourly

    def run(self):
        # todo: run wl through models: one-off, eager, lazy, hybrid
        one_off_model = OneOffExecutionModel(self.wl)
        # eager_model = EagerExecutionModel(self.wl, self.cache_config)
        # lazy_model = LazyExecutionModel(self.wl, self.cache_config)
        hybrid_model = HybridExecutionModel(self.wl, self.cache_config, self.load_ref)

        one_off_plan = one_off_model.generate_workload_execution_plan()
        # eager_plan = eager_model.generate_workload_execution_plan()
        # lazy_plan = lazy_model.generate_workload_execution_plan()
        hybrid_plan = hybrid_model.generate_workload_execution_plan()

        one_off_plan["load"] = one_off_plan.apply(lambda q: estimate_query_load(q, self.load_ref), axis=1)
        # eager_plan["load"] = eager_plan.apply(lambda q: estimate_query_load(q, self.load_ref), axis=1)
        # lazy_plan["load"] = lazy_plan.apply(lambda q: estimate_query_load(q, self.load_ref), axis=1)

        one_off_hourly = self.get_hourly(one_off_plan)
        # eager_plan_hourly = self.get_hourly(eager_plan)
        # lazy_plan_hourly = self.get_hourly(lazy_plan)
        hybrid_plan_hourly = self.get_hourly(hybrid_plan)

        one_off_hourly.to_csv("results/one_off_hourly.csv")
        one_off_plan.to_csv("results/one_off_plan.csv")
        # eager_plan_hourly.to_csv("results/eager_hourly.csv")
        # lazy_plan_hourly.to_csv("results/lazy_hourly.csv")
        hybrid_plan.to_csv("results/hybrid_plan.csv")
        hybrid_plan_hourly.to_csv("results/hybrid_hourly.csv")

        self.plot(one_off_hourly, "one-off.png")
        # self.plot(eager_plan_hourly, "eager.png")
        self.plot(hybrid_plan_hourly, "hybrid.png")


    def plot(self, hourly_wl, file_name):
        plot = (
                ggplot(hourly_wl, aes(x="hour", y="load", fill="type")) +
                geom_bar(stat="identity", position="stack", width=0.5) +
                labs(title=f"WL Elasticity",
                     x="Hours",
                     y="Resource Requirement Score") +
                scale_y_continuous(limits=(0, None)) +
                ylim(0, 1500) +
                theme_bw() +
                scale_fill_manual(values={'Write': '#1f77b4', 'Read-Only': '#ff7f0e'}) +
                theme(
                    legend_position='right',
                    subplots_adjust={'wspace': 0.25},
                    axis_text_x=element_text(rotation=45, hjust=1),
                    figure_size=(5, 3),
                    panel_spacing=0.6,
                    legend_title=element_blank()
                )
        )

        plot.save(filename=f"{file_name}", path=self.base_config["output_path"])

if __name__ == "__main__":
    base_config = load_json("config.json")
    experiment = WorkloadHourlyElasticityExperiment(base_config)
    experiment.run()