from utils.file import load_json
from workload_analyzer.redset_workload_extractor import RedsetWorkloadExtractor
from workload_generator.generator import WorkloadGenerator

if __name__ == "__main__":
    extractor = RedsetWorkloadExtractor(
        cluster_id=1,
    )

    base_config = load_json("config.json")
    config = extractor.export_config(base_config)
    wl_generator = WorkloadGenerator(config)
    wl = wl_generator.generate_workload()

    wl.to_csv("results/wl_hourly.csv", index=False)
