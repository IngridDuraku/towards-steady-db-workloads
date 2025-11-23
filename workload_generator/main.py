from pathlib import Path

from utils.file import load_json, save_json_file
from workload_analyzer.workload_insights import WorkloadInsights
from workload_generator.generator import WorkloadGenerator

if __name__ == "__main__":
    script_dir = Path(__file__).parent
    config_path = script_dir / "config.json"
    config = load_json(config_path)

    name = config["workload_name"]

    wl_generator = WorkloadGenerator(config)
    wl = wl_generator.generate_workload()
    insights = WorkloadInsights(wl).get_insights()

    directory = script_dir / f"workloads/{name}"
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)

    wl.to_csv(f"{directory}/wl.csv", index=False)
    save_json_file(insights, f"{directory}/insights.json")
    save_json_file(config, f"{directory}/config.json")