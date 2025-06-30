from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn.objects as so
from matplotlib import image as mpimg

from pricing_calculator.basic_runtime_estimator import BasicRuntimeEstimator


def hourly_plot_all_models_for_cluster(one_off_plan, eager_plan, lazy_plan, hybrid_plan, output_dir):
    one_off_plan["model"] = "one-off"
    eager_plan["model"] = "eager"
    lazy_plan["model"] = "lazy"
    hybrid_plan["model"] = "hybrid"
    threshold = hybrid_plan["threshold"].iloc[0]

    df = pd.concat([one_off_plan, lazy_plan, hybrid_plan], ignore_index=True)
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
            .facet("model", order=["one-off", "lazy", "hybrid"])
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

        p.save(f"{directory}/plot_by_{group}.png", bbox_inches='tight')

    img1 = mpimg.imread(f"{directory}/plot_by_query_type.png")
    img2 = mpimg.imread(f"{directory}/plot_by_execution.png")
    img3 = mpimg.imread(f"{directory}/plot_by_execution_trigger.png")

    # Combine in one figure
    fig, axs = plt.subplots(3, 1, figsize=(12, 12))

    for ax, img in zip(axs, [img1, img2, img3]):
        ax.imshow(img)
        ax.axis('off') # hide axes

    plt.tight_layout()
    plt.show()
    fig.savefig(f"{directory}/combined_plot.png", bbox_inches='tight')


def get_latency_props(plan, hw_params):
    def get_query_latency(q):
        mask1 = plan["triggered_by"] == q["query_hash"]
        mask2 = plan["query_hash"] != q["query_hash"]
        triggered_q = plan[mask2 & mask1]

        return q["runtime"] + triggered_q["runtime"].sum()

    plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_params, plan)

    mask1 = plan["execution_trigger"] == "immediate"
    mask2 = plan["query_type"] == "select"

    immediate_queries = plan[mask1 & mask2]

    immediate_queries.loc[:, "latency"] = immediate_queries.apply(lambda q: get_query_latency(q), axis=1)

    latency = {
        "mean": immediate_queries["latency"].mean(),
        "max": immediate_queries["latency"].max(),
        "min": immediate_queries["latency"].min(),
        "q25": immediate_queries["latency"].quantile(0.25),
        "q75": immediate_queries["latency"].quantile(0.25),
        "q50": immediate_queries["latency"].quantile(0.5),
        "std": immediate_queries["latency"].std()
    }

    return latency

def get_cost_props(model, hw_params):
    compute_cost = model.get_compute_cost(hw_params)
    storage_cost = model.get_storage_cost(hw_params)

    put_requests = 0
    get_requests = 0
    usage = 0

    if model.cache:
        put_requests = model.cache.insights["put_requests"]
        get_requests = model.cache.insights["get_requests"]
        usage = model.cache.usage


    cost = {
        "compute": compute_cost,
        "storage": {
            "cost": storage_cost,
            "usage": usage,
            "put_requests": put_requests,
            "get_requests": get_requests,
        },
        "total": compute_cost + storage_cost
    }

    return cost

def estimate_latency(plan, hw_params):
    def get_query_latency(q):
        if q["query_type"] != "select" or q["execution_trigger"] != "immediate":
            return None

        mask1 = plan["triggered_by"] == q["query_hash"]
        mask2 = plan["query_hash"] != q["query_hash"]
        triggered_q = plan[mask2 & mask1]

        return q["runtime"] + triggered_q["runtime"].sum()

    plan["runtime"] = BasicRuntimeEstimator.estimate_runtime_per_query(hw_params, plan)

    # mask1 = plan["execution_trigger"] == "immediate"
    # mask2 = plan["query_type"] == "select"
    #
    # immediate_queries = plan[mask1 & mask2]

    latency = plan.apply(lambda q: get_query_latency(q), axis=1)

    return latency
