import hashlib


def generate_hash(*values):
    combined = "-".join(map(str, values)).encode('utf-8')
    return hashlib.md5(combined).hexdigest()
