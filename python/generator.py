import hashlib
import json
import random
import string
from faker import Faker

fake = Faker()

def generate_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=8))


def generate_blob(blob_id, num_targets):
    random.seed(blob_id)  # Seeding with blob ID for deterministic randomness

    name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    size = random.randint(1, 1000)
    score = random.randint(1, 100)
    access_frequency = round(random.uniform(0, 1), 2)

    random.seed()  # Resetting the seed

    target_id = random.randint(0, num_targets-1)

    buffer_info = {
        "target_id": target_id,
        "size": size
    }

    return {
        "id": blob_id,
        "name": name,
        "score": score,
        "access_frequency": access_frequency,
        "buffer_info": buffer_info
    }


def generate_bucket(num_blobs, num_traits, bucket_id):
    random.seed(bucket_id)
    name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    random.seed()
    traits = [''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
              for _ in range(random.randint(0, num_traits))]
    blob_ids = random.sample(range(num_blobs), k=random.randint(1, num_blobs))

    return {
        "name": name,
        "id": bucket_id,
        "traits": traits,
        "blobs": blob_ids
    }


def generate_target(node_id, target_type, target_id):
    remaining_capacity = round(random.uniform(0, 1), 2)
    return {
        "name": target_type,
        "id": target_id,
        "remaining_capacity": remaining_capacity,
        "node_id": node_id
    }

def compute_hash(d):
    json_str = json.dumps(d, sort_keys=True)
    hash_obj = hashlib.sha256(json_str.encode())
    return hash_obj.hexdigest()

def generate_metadata(num_buckets=5, num_blobs=10, num_targets=4, num_nodes=32, num_traits=2):
    buckets = [generate_bucket(num_blobs, num_traits, bucket_id) for bucket_id in range(num_buckets)]
    blobs = [generate_blob(blob_id, num_targets * num_nodes) for blob_id in range(num_blobs)]

    # Define target types
    target_types = ["Memory", "NVme", "BB", "Pfs"]

    # Generate targets
    targets = []
    for node_number in range(1, num_nodes + 1):
        node_id = "ares-comp-" + str(node_number).zfill(2)
        for t in range(num_targets):
            targets.append(generate_target(node_id, target_types[t], len(targets)))

    combined_hash = hashlib.sha256((compute_hash(buckets) + compute_hash(blobs) +
                                    compute_hash(targets)).encode()).hexdigest()

    return {
        "id": combined_hash,
        "buckets": buckets,
        "blobs": blobs,
        "targets": targets
    }


if __name__ == '__main__':
    metadata = generate_metadata(num_buckets=2, num_blobs=6, num_nodes=4, num_traits=2, num_targets=4)
    print(json.dumps(metadata, indent=4))
