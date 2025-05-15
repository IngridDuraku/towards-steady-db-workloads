import numpy as np

from utils.common import generate_hash
from workload_generator.query_generator.statistical_helpers import compute_lognormal_params


class QueryGenerator:
    def __init__(self, config):
        self.config = config

    def generate_query(self):
        type_p = np.array(list(self.config["query_type_p"].values()))
        type_p /= type_p.sum()
        q_type = np.random.choice(
            list(self.config["query_type_p"].keys()),
            p= type_p
        )
        # bytes_scanned
        bytes_scanned = self.config["bytes_scanned"]
        lb = bytes_scanned["lower_bound_mb"]
        up = bytes_scanned["upper_bound_gb"]
        mu, sigma = compute_lognormal_params(lb, up)
        q_bytes_scanned = int(np.random.lognormal(mean=mu, sigma=sigma))

        # num read tables
        values = self.config["read_tables_distribution"].index
        p = self.config["read_tables_distribution"].values
        q_num_read_tables = np.random.choice(values, p=p)

        # result_size
        if q_type == "select":
            result_size = self.config["result_size"]
            lb = result_size["lower_bound_mb"]
            up = result_size["upper_bound_gb"]
            mu, sigma = compute_lognormal_params(lb, up)
            q_result_size = int(np.random.lognormal(mu, sigma))
        else:
            q_result_size = 0

        # intermediate_result_size
        q_ir_size = self.estimate_intermediate_results_size(
            query_type=q_type,
            bytes_scanned=q_bytes_scanned,
            result_size=q_result_size,
            num_read_tables=q_num_read_tables,
        )

        # write_volume
        write_volume_scale = {
            "select": 0,
            "insert": 1.0,
            "delete": 0.01,
            "update": 0.1,
        }
        write_volume = self.config["write_volume"]
        lb = write_volume["lower_bound_mb"]
        up = write_volume["upper_bound_gb"]
        mu, sigma = compute_lognormal_params(lb, up)
        q_write_volume = int(np.random.lognormal(mu, sigma)) * write_volume_scale.get(q_type)

        # cpu_time
        factors = {
            "bs": 1e-9,  # ms per byte
            "rs": 1e-8,
            "wv": 1e-8,
        }
        jitter = np.random.gamma(2.0, 2.0)
        q_cpu_time = (factors["bs"] * q_bytes_scanned +
                      factors["rs"] * q_result_size +
                      factors["wv"] * q_write_volume +
                      jitter) / 1000  # convert to seconds

        # specify unique_db_instance uniformly
        q_db_id = np.random.randint(low=0, high=self.config["db_count"])

        # generate query_hash
        q_hash = generate_hash(
            q_type,
            q_bytes_scanned,
            q_result_size,
            q_write_volume,
            q_cpu_time,
            q_db_id,
            q_num_read_tables
        )

        return {
            "query_hash": q_hash,
            "query_type": q_type,
            "bytes_scanned": q_bytes_scanned,
            "result_size": q_result_size,
            "write_volume": q_write_volume,
            "cpu_time": q_cpu_time,
            "unique_db_instance": q_db_id,
            "num_read_tables": q_num_read_tables,
            "intermediate_result_size": q_ir_size,
        }

    def estimate_intermediate_results_size(
            self,
            query_type,
            bytes_scanned,
            result_size,
            num_read_tables,
    ):
        query_type_multipliers = {
            'select': 2.0,
            'update': 1.8,
            'insert': 1.2,
            'delete': 1.5
        }
        scale = self.config["ir_scale"]

        query_type_factor = query_type_multipliers.get(query_type, 1.5)
        read_tables_factor = 1 + (num_read_tables - 1) * 0.5

        # reduction ratio (how much the data was reduced from scan to result)
        if bytes_scanned > 0:
            reduction_ratio = result_size / bytes_scanned
        else:
            reduction_ratio = 1.0

        intermediate_size = bytes_scanned * query_type_factor * read_tables_factor * reduction_ratio \
                            * 10 ** scale

        return round(intermediate_size)
