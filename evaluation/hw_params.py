HW_PARAMETERS = {
  "aws_instances": {
    "c5n.large": {
      "vCPUs": 2,
      "network_speed": 25,
      "price_per_hour": 0.108
    }
  },
  "cache": {
    "s3": {
      "cost_per_gb": 0.023,
      "put_cost": 0.005,
      "get_cost": 0.0004,
      "request_latency_min": 100,
      "request_latency_max": 200,
      "type": "s3",
      "extra_cost": 0
    },
    "gp3":{
      "cost_per_gb": 0.081,
      "put_cost": 0,
      "get_cost": 0,
      "request_latency_min": 1,
      "request_latency_max": 2,
      "type": "gp3",
      "throughput_mb_per_s": 125,
      "extra_cost": 0
    }
  }
}
