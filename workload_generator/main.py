from utils.file import load_json
from workload_generator.generator import WorkloadGenerator

if __name__ == "__main__":
    config_path = "config.json"
    config = load_json(config_path)

    wl_generator = WorkloadGenerator(config)

    wl = wl_generator.generate_workload()
    wl.to_csv("results/wl_hourly.csv", index=False)