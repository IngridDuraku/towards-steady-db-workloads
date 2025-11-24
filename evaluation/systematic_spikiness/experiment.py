from pathlib import Path

import pandas as pd

from evaluation.hw_params import HW_PARAMETERS
from evaluation.utils import get_latency_props
from execution_model.models.eager import EagerExecutionModel
from execution_model.models.hybrid import HybridModel
from execution_model.models.lazy import LazyExecutionModel
from execution_model.models.one_off import OneOffExecutionModel
from pricing_calculator.basic_runtime_estimator import BasicRuntimeEstimator
from utils.file import load_json, save_json_file
from utils.workload import estimate_query_load
from workload_analyzer.workload_insights import WorkloadInsights
from workload_generator.generator import WorkloadGenerator
from pathlib import Path
from matplotlib import image as mpimg, pyplot as plt
import seaborn.objects as so


class SystematicSpikiness:
    def __init__(self):
        config_path = "config.json"
        self.config = load_json(config_path)
        self.cache_params = HW_PARAMETERS["cache"]["gp3"]
        self.cache_config = {
            "max_capacity": 1e9,
            "cost_per_gb": self.cache_params["cost_per_gb"],
            "put_cost": self.cache_params["put_cost"],
            "get_cost": self.cache_params["get_cost"],
            "cache_type": "gp3"
        }
        self.hw_params = {
            "instance": HW_PARAMETERS["aws_instances"]["c5n.large"],
            "cache": self.cache_params
        }

    def plot_load(self, one_off_plan, lazy_plan, eager_plan, hybrid_plan, output_dir):
        one_off_plan["model"] = "one-off"
        eager_plan["model"] = "eager"
        lazy_plan["model"] = "lazy"
        hybrid_plan["model"] = "hybrid"
        threshold = hybrid_plan["threshold"].iloc[0]

        df = pd.concat([one_off_plan, eager_plan, lazy_plan, hybrid_plan], ignore_index=True)
        is_read = df["query_type"] == "select"
        is_write = ~is_read

        df.loc[is_read, "query_type"] = "Read-Only"
        df.loc[is_write, "query_type"] = "Write"

        models = ["one-off", "eager", "lazy", "hybrid"]
        group_labels = ["query_type", "execution", "execution_trigger"]
        group_titles = ["By Query Type", "By Execution Mode", "By Execution Trigger"]

        palettes = ["Set2", "Paired", "Dark2"]

        directory = f"{output_dir}"
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)

        for x, group in enumerate(group_labels):
            data = df.groupby(["hour", group, "model"])["load"].sum().reset_index()

            p = (
                so.Plot(data, x="hour", y="load")
                .add(so.Bars(), so.Stack(), color=group)
                .facet("model", order=["one-off", "eager", "lazy", "hybrid"])
                .add(so.Line(color="black", linestyle="--"), data=pd.DataFrame({
                    "hour": data["hour"],
                    "load": [threshold] * len(data["hour"])
                }))
                .theme({"figure.figsize": (4 * 3, 4)})
                .scale(color=palettes[x])
                .label(
                    color=group_titles[x],
                    x="Hour",
                    y="Resource Requirement Score"
                )
            )

            p.save(f"{directory}/plot_load_by_{group}.png", bbox_inches='tight')

        img1 = mpimg.imread(f"{directory}/plot_load_by_query_type.png")
        img2 = mpimg.imread(f"{directory}/plot_load_by_execution.png")
        img3 = mpimg.imread(f"{directory}/plot_load_by_execution_trigger.png")

        # Combine in one figure
        fig, axs = plt.subplots(3, 1, figsize=(12, 12))

        for ax, img in zip(axs, [img1, img2, img3]):
            ax.imshow(img)
            ax.axis('off')  # hide axes

        plt.tight_layout()
        plt.show()
        fig.savefig(f"{directory}/load_combined_plot.png", bbox_inches='tight')

    def plot_runtime(self, one_off_plan, lazy_plan, eager_plan, hybrid_plan, output_dir):
        one_off_plan["model"] = "one-off"
        eager_plan["model"] = "eager"
        lazy_plan["model"] = "lazy"
        hybrid_plan["model"] = "hybrid"

        df = pd.concat([one_off_plan, eager_plan, lazy_plan, hybrid_plan], ignore_index=True)
        is_read = df["query_type"] == "select"
        is_write = ~is_read

        df.loc[is_read, "query_type"] = "Read-Only"
        df.loc[is_write, "query_type"] = "Write"

        models = ["one-off", "eager", "lazy", "hybrid"]
        group_labels = ["query_type", "execution", "execution_trigger"]
        group_titles = ["By Query Type", "By Execution Mode", "By Execution Trigger"]

        palettes = ["Set2", "Paired", "Dark2"]

        directory = f"{output_dir}"
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)

        data = df[df["model"] == "hybrid"].groupby(["hour", "execution_trigger", "model"])[
            "runtime"].sum().reset_index()
        deferred_hours = data[data["execution_trigger"] == "deferred"]["hour"].to_list()

        mask = data["hour"].apply(lambda x: x in deferred_hours)
        deferred_hours_total_runtime = data[mask].groupby("hour").sum().reset_index()
        print(deferred_hours)
        print(mask)
        threshold = deferred_hours_total_runtime["runtime"].max()

        for x, group in enumerate(group_labels):
            data = df.groupby(["hour", group, "model"])["runtime"].sum().reset_index()

            p = (
                so.Plot(data, x="hour", y="runtime")
                .add(so.Bars(), so.Stack(), color=group)
                .facet("model", order=["one-off", "eager", "lazy", "hybrid"])
                .add(so.Line(color="black", linestyle="--"), data=pd.DataFrame({
                    "hour": data["hour"],
                    "runtime": [threshold] * len(data["hour"])
                }))
                .theme({"figure.figsize": (4 * 3, 4)})
                .scale(color=palettes[x])
                .label(
                    color=group_titles[x],
                    x="Hour",
                    y="Runtime (s)"
                )
            )

            p.save(f"{directory}/plot_runtime_by_{group}.png", bbox_inches='tight')

        img1 = mpimg.imread(f"{directory}/plot_runtime_by_query_type.png")
        img2 = mpimg.imread(f"{directory}/plot_runtime_by_execution.png")
        img3 = mpimg.imread(f"{directory}/plot_runtime_by_execution_trigger.png")

        # Combine in one figure
        fig, axs = plt.subplots(3, 1, figsize=(12, 12))

        for ax, img in zip(axs, [img1, img2, img3]):
            ax.imshow(img)
            ax.axis('off')  # hide axes

        plt.tight_layout()
        plt.show()
        fig.savefig(f"{directory}/runtime_combined_plot.png", bbox_inches='tight')

    def run(self):
        name = self.config["name"]
        result_path = f"results/workload_{name}"
        path = Path(result_path)
        path.mkdir(parents=True, exist_ok=True)

        result = []

        wl_generator = WorkloadGenerator(self.config)
        wl = wl_generator.generate_workload()
        insights = WorkloadInsights(wl).get_insights()

        load_ref = {
            "bytes_scanned": wl["bytes_scanned"].max(),
            "result_size": wl["result_size"].max(),
            "write_volume": wl["write_volume"].max(),
            "cpu_time": wl["cpu_time"].max(),
        }

        wl.to_csv(f"{result_path}/wl.csv", index=False)
        save_json_file(insights, f"{result_path}/insights.json")
        save_json_file(self.config, f"{result_path}/config.json")
        save_json_file(self.hw_params, f"{result_path}/hw_params.json")
        save_json_file(self.cache_config, f"{result_path}/cache_config.json")

        # run one-off
        one_off = OneOffExecutionModel(wl)
        one_off_plan = one_off.generate_workload_execution_plan()
        one_off_runtime = one_off.get_runtime(hw_parameters=self.hw_params)
        one_off_cost = one_off.get_cost(hw_parameters=self.hw_params)
        one_off_plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters=self.hw_params,
                                                                                   wl=one_off_plan)
        one_off_plan.loc[:, "load"] =  one_off_plan.apply(lambda q: estimate_query_load(q, load_ref), axis=1)

        one_off_insights = WorkloadInsights(one_off_plan).get_insights()
        one_off_latency = get_latency_props(one_off_plan, self.hw_params)
        one_off_plan.to_csv(f"{result_path}/one_off_plan.csv")

        one_off_item ={
            "cost": {
                "total_cost": one_off_cost,
                "pending_cost": 0,
                "compute_cost": 0,
                "storage_cost": 0
            },
            "runtime": one_off_runtime,
            "cost_reduction": 0,
            "speedup": 0,
            "cache": {
                "usage": 0,
                "insights": 0
            },
            "latency": one_off_latency,
            "workload_insights": one_off_insights
        }

        print("Hybrid Running")
        hybrid_model = HybridModel(wl, self.cache_config, load_ref)
        hybrid_plan = hybrid_model.generate_workload_execution_plan()
        pending_cost = hybrid_model.get_pending_cost(self.hw_params)
        hybrid_cost = hybrid_model.get_cost(self.hw_params)
        hybrid_runtime = hybrid_model.get_runtime(hw_parameters=self.hw_params)
        hybrid_plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters=self.hw_params,
                                                                                   wl=hybrid_plan)
        hybrid_speedup = (one_off_runtime - hybrid_runtime) / one_off_runtime
        hybrid_cost_reduction = (one_off_cost - hybrid_cost) / one_off_cost
        hybrid_insights = WorkloadInsights(hybrid_plan).get_insights()
        hybrid_latency = get_latency_props(hybrid_plan, self.hw_params)

        hybrid_item = {
            "cost": {
                "total_cost": hybrid_cost,
                "pending_cost": pending_cost,
                "compute_cost": hybrid_model.get_compute_cost(self.hw_params),
                "storage_cost": hybrid_model.get_storage_cost(self.hw_params)
            },
            "runtime": hybrid_runtime,
            "cost_reduction": hybrid_cost_reduction,
            "speedup": hybrid_speedup,
            "cache": {
                "usage": hybrid_model.cache.usage,
                "insights": hybrid_model.cache.insights
            },
            "latency": hybrid_latency,
            "workload_insights": hybrid_insights
        }
        hybrid_plan.to_csv(f"{result_path}/hybrid_plan.csv")

        print("Lazy Running")
        lazy_model = LazyExecutionModel(wl, self.cache_config)
        lazy_plan = lazy_model.generate_workload_execution_plan()
        pending_cost = lazy_model.get_pending_cost(self.hw_params)
        lazy_cost = lazy_model.get_cost(self.hw_params)
        lazy_runtime = lazy_model.get_runtime(hw_parameters=self.hw_params)
        lazy_plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters=self.hw_params,
                                                                                   wl=lazy_plan)
        lazy_plan.loc[:, "load"] =  lazy_plan.apply(lambda q: estimate_query_load(q, load_ref), axis=1)
        lazy_speedup = (one_off_runtime - lazy_runtime) / one_off_runtime
        lazy_cost_reduction = (one_off_cost - lazy_cost) / one_off_cost
        lazy_insights = WorkloadInsights(lazy_plan).get_insights()
        lazy_latency = get_latency_props(lazy_plan, self.hw_params)

        lazy_item = {
            "cost": {
                "total_cost": lazy_cost,
                "pending_cost": pending_cost,
                "compute_cost": lazy_model.get_compute_cost(self.hw_params),
                "storage_cost": lazy_model.get_storage_cost(self.hw_params)
            },
            "runtime": lazy_runtime,
            "cost_reduction": lazy_cost_reduction,
            "speedup": lazy_speedup,
            "cache": {
                "usage": lazy_model.cache.usage,
                "insights": lazy_model.cache.insights
            },
            "latency": lazy_latency,
            "workload_insights": lazy_insights
        }
        lazy_plan.to_csv(f"{result_path}/lazy_plan.csv")

        print("Eager Running")
        eager_model = EagerExecutionModel(wl, self.cache_config)
        eager_plan = eager_model.generate_workload_execution_plan()
        eager_cost = eager_model.get_cost(self.hw_params)
        eager_runtime = eager_model.get_runtime(hw_parameters=self.hw_params)
        eager_plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_parameters=self.hw_params,
                                                                                   wl=eager_plan)
        eager_plan.loc[:, "load"] =  eager_plan.apply(lambda q: estimate_query_load(q, load_ref), axis=1)


        eager_speedup = (one_off_runtime - eager_runtime) / one_off_runtime
        eager_cost_reduction = (one_off_cost - eager_cost) / one_off_cost

        eager_insights = WorkloadInsights(eager_plan).get_insights()
        eager_latency = get_latency_props(eager_plan, self.hw_params)

        eager_item = {
            "cost": {
                "total_cost": eager_cost,
                "pending_cost": 0,
                "compute_cost": eager_model.get_compute_cost(self.hw_params),
                "storage_cost": eager_model.get_storage_cost(self.hw_params)
            },
            "runtime": eager_runtime,
            "cost_reduction": eager_cost_reduction,
            "speedup": eager_speedup,
            "cache": {
                "usage": eager_model.cache.usage,
                "insights": eager_model.cache.insights
            },
            "latency": eager_latency,
            "workload_insights": eager_insights
        }
        eager_plan.to_csv(f"{result_path}/eager_plan.csv")

        result = {
            "one_off": one_off_item,
            "eager": eager_item,
            "lazy": lazy_item,
            "hybrid": hybrid_item,
        }

        save_json_file(result, f"{result_path}/result.json")

        self.plot_runtime(one_off_plan, lazy_plan, eager_plan, hybrid_plan, f"{result_path}/runtime")
        self.plot_load(one_off_plan, lazy_plan, eager_plan, hybrid_plan, f"{result_path}/load")


if __name__ == "__main__":
    experiment = SystematicSpikiness()
    experiment.run()