from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import seaborn.objects as so
from matplotlib import image as mpimg

def hourly_plot_all_models_for_cluster(cluster_id):
    one_off_plan = pd.read_csv(f"results/c{cluster_id}/one_off_plan.csv")
    one_off_plan["model"] = "one-off"

    eager_plan = pd.read_csv(f"results/c{cluster_id}/eager_plan.csv")
    eager_plan["model"] = "eager"

    lazy_plan = pd.read_csv(f"results/c{cluster_id}/lazy_plan.csv")
    lazy_plan["model"] = "lazy"

    hybrid_plan = pd.read_csv(f"results/c{cluster_id}/hybrid_plan.csv")
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

    directory = f"plots/c{cluster_id}"
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


if __name__ == '__main__':
    clusters = [7, 8, 9, 10, 11, 12, 13, 14]
    for id in clusters:
        hourly_plot_all_models_for_cluster(id)



#
# def hourly_plot_all_models_for_cluster(cluster_id):
#     one_off_plan = pd.read_csv(f"results/c{cluster_id}/one_off_plan.csv")
#     eager_plan = pd.read_csv(f"results/c{cluster_id}/eager_plan.csv")
#     lazy_plan = pd.read_csv(f"results/c{cluster_id}/lazy_plan.csv")
#     hybrid_plan = pd.read_csv(f"results/c{cluster_id}/hybrid_plan.csv")
#
#     datasets = [one_off_plan, eager_plan, lazy_plan, hybrid_plan]
#     titles = ['One-Off', 'Eager', 'Lazy', 'Hybrid']
#     group_keys = ['query_type', 'execution', 'execution_trigger']
#     row_titles = ['By Query Type', 'By Execution Mode', 'By Execution Trigger']
#     # Define different color palettes for each row
#     palettes = [
#         sns.color_palette("Set2"),
#         sns.color_palette("Paired"),
#         sns.color_palette("Dark2")
#     ]
#
#     # Recreate the plot with custom palettes and formatting
#     fig, axes = plt.subplots(nrows=3, ncols=4, figsize=(20, 12), sharex=True, sharey='row')
#     sns.set_theme(style="ticks")  # Remove grid lines
#
#     # Store legend handles and labels for each row
#     legend_info = {
#         # 0: ["select", "insert", "update", "delete"],
#         # 1: ["normal", "incremental"],
#         # 2: ["immediate", "deferred", "triggered_by_read", "triggered_by_write"],
#     }
#
#     for col, (df, model_title) in enumerate(zip(datasets, titles)):
#         for row, (group_key, row_title) in enumerate(zip(group_keys, row_titles)):
#             ax = axes[row, col]
#             grouped = df.groupby(['hour', group_key])['load'].sum().reset_index()
#             pivoted = grouped.pivot(index='hour', columns=group_key, values='load').fillna(0)
#             pivoted = pivoted.sort_index()
#
#             # Use custom palette
#             colors = palettes[row][:pivoted.shape[1]]
#             pivoted.plot(kind='bar', stacked=True, ax=ax, legend=False, color=colors)
#
#             # Remove grid lines
#             ax.grid(False)
#
#             xticks = np.linspace(1, ax.get_xlim()[1], 5)
#             ax.set_xticks(xticks.astype(int))
#             ax.set_xticklabels(xticks)
#
#             # Y-axis: only show 3 values
#             yticks = np.linspace(0, ax.get_ylim()[1], 3)
#             ax.set_yticks(yticks.astype(int))
#
#             # Labels and titles
#             if col == 0:
#                 ax.set_ylabel("Total Load")
#             else:
#                 ax.set_ylabel("")
#
#             if row == 2:
#                 ax.set_xlabel("Hour")
#             else:
#                 ax.set_xlabel("")
#
#             if row == 0:
#                 ax.set_title(model_title)
#
#             if col == 3:
#                 handles, labels = ax.get_legend_handles_labels()
#                 legend_info[row] = (handles, labels)
#
#
#     # Add titles above each row
#     for row, row_title in enumerate(row_titles):
#         axes[row, 0].annotate(row_title, xy=(0, 0.5), xytext=(-axes[row, 0].yaxis.labelpad - 10, 0),
#                               xycoords=axes[row, 0].yaxis.label, textcoords='offset points',
#                               ha='right', va='center', fontsize=14, fontweight='bold', rotation=90)
#
#     # Add separate legends for each row
#     for row in range(3):
#         handles, labels = legend_info[row]
#         axes[row, -1].legend(handles, labels, loc='upper left', bbox_to_anchor=(1.05, 1), title=row_titles[row])
#
#     # Save updated figure
#     directory = f"plots/c{cluster_id}"
#     path = Path(directory)
#     path.mkdir(parents=True, exist_ok=True)
#
#
#     plt.tight_layout(rect=[0, 0, 1, 0.97])
#     fig.savefig(f"{directory}/plot.png", dpi=300, bbox_inches='tight')
#     plt.show()