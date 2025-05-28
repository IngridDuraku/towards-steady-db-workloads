from pathlib import Path

from utils.file import load_json, save_json_file
from workload_analyzer.redset_workload_extractor import RedsetWorkloadExtractor
from workload_analyzer.workload_insights import WorkloadInsights
from workload_generator.generator import WorkloadGenerator

if __name__ == "__main__":
    cluster_id = 2
    extractor = RedsetWorkloadExtractor(
        cluster_id=cluster_id,
    )

    base_config = load_json("config.json")
    config = extractor.export_config(base_config)
    wl_generator = WorkloadGenerator(config)
    wl = wl_generator.generate_workload()
    insights = WorkloadInsights(wl).get_insights()

    directory = f"results/c{cluster_id}"
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)

    wl.to_csv(f"{directory}/wl.csv", index=False)
    save_json_file(insights, f"{directory}/insights.json")
    save_json_file(config, f"{directory}/config.json")
