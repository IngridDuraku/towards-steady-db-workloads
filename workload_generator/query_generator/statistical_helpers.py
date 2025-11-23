from math import log


def compute_lognormal_params(lower_bound, upper_bound, percentile=95, skew_factor=0.5):
    lb = lower_bound * 1e6  # MB to bytes
    ub = upper_bound * 1e9  # GB to bytes

    # Central tendency = log_e of geometric mean
    # Span width = percentile width in log space (e.g., 95% = 2 sigma for normal)
    log_lb = log(lb)
    log_ub = log(ub)
    spread = (log_ub - log_lb) / 2

    # Adjust to match target percentile range (assuming 2 sigma for 95%)
    sigma = spread / 2  # 2 sigma - 95% = sigma = spread / 2

    mu = log_lb + skew_factor * (log_ub - log_lb)

    return mu, sigma

def compute_gamma_params(lower_bound, upper_bound, percentile=95):
    lb = lower_bound * 1e3  # KB to bytes
    ub = upper_bound * 1e6  # MB to bytes

    # Approximate mean using geometric mean
    mean = (lb * ub)**0.5

    # Target variance so that 95% is within bounds
    std_dev = (ub - lb) / 4
    variance = std_dev**2

    # mean = alpha * theta, var = alpha * theta => solve:
    alpha = mean**2 / variance
    theta = variance / mean

    return alpha, theta