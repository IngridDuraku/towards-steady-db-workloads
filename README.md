# IVM Strategy Simulation Engine

An engine for evaluating different Incremental View Maintenance (IVM) strategies under realistic and synthetic workload conditions.  
It provides a simple, zero-cost way to study performance, runtime, and resource usage **without executing queries on a real cloud database**.

---

## Purpose

Modern analytical workloads often experience sudden spikes in query demand. To mitigate these spikes, different IVM strategies—**One-Off**, **Eager**, **Lazy**, and **Hybrid**—can be used to reduce recomputation cost and smooth resource usage.

This simulation engine enables:

- Prototyping and comparison of IVM strategies  
- Testing under a variety of workload patterns  
- Exploration of hardware configurations  
- Cost and runtime estimation without cloud execution  
- Reproducible experiments at scale  

It serves as an experimental platform to validate ideas before running expensive real-system experiments.

---

## Key Features

- **Execution Models:** Supports One-Off, Eager, Lazy, and Hybrid maintenance strategies to evaluate their impact on performance, cost and workload stability.
- **Cost Model:** Includes a configurable cost model that estimates CPU, I/O, memory, and maintenance overhead based on Redshift-derived metadata.
- **Hardware Profiles:** Allows plugging in different hardware configurations to approximate performance and cost on various cloud environments.
- **Workload Generation:** Generates synthetic workloads or imports realistic workloads derived from Redshift cluster traces (Redset).
- **Runtime & Cost Estimation:** Produces per-query and aggregated metrics for execution time, maintenance time, cache usage, and overall cost.


### Prerequisites

- Python 3.10+
- pip or conda for package management


### Setup

1. **Install dependencies**
```bash
   pip install -r requirements.txt
```

2. **Generate workload**
   
   Configure your workload parameters in `workload_generator/config.json`, then run:
```bash
    python3 -m workload.generate_wl
```
This will create a directory with the specified `workload_name` inside `workload_generator/workloads/` that includes:
- `workload.csv` - The generated workload queries
- `config.json` - The workload configuration parameters
- `insights.json` - Generated workload characteristics.

To generate a workload based on Redset cluster traces run: 

```bash
    python3 -m workload.generate_wl
```


### 3. Run an Experiment

- Each experiment is located under the `evaluation/` directory.

- If the experiment requires workload data, place it in: evaluation/<experiment_name>/data/

- Run the experiment by executing its main script: evaluation/<experiment_name>/experiment.py

- All output files will be generated under: evaluation/<experiment_name>/results/