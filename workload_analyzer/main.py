import argparse
from pathlib import Path

from utils.file import load_json, save_json_file
from workload_analyzer.redset_workload_extractor import RedsetWorkloadExtractor
from workload_analyzer.workload_insights import WorkloadInsights
from workload_generator.generator import WorkloadGenerator

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate workload based on redset cluster traces")
    parser.add_argument(
        "--cluster_id",
        type=str,
        required=True,
        help="ID of the cluster"
    )
    args = parser.parse_args()

    cluster_id = args.cluster_id

    print("Generating workloads for cluster {}".format(cluster_id))
    extractor = RedsetWorkloadExtractor(
        cluster_id=cluster_id,
    )

    config_path =  Path(__file__).parent / "config.json"
    base_config = load_json(config_path)
    config = extractor.export_config(base_config)
    print("Size: ", config["size"])

    wl_generator = WorkloadGenerator(config)
    wl = wl_generator.generate_workload()
    insights = WorkloadInsights(wl).get_insights()

    directory =  Path(__file__).parent / f"cluster_workloads/c{cluster_id}"
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)

    wl.to_csv(f"{directory}/wl.csv", index=False)
    save_json_file(insights, f"{directory}/insights.json")
    save_json_file(config, f"{directory}/config.json")
