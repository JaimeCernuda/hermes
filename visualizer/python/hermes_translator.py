import hashlib
import json


class HermesTranslator:
    def __init__(self, hermes_data):
        self.hermes_data = hermes_data

        buckets = []
        blobs = []
        targets = []

        for bucket in self.hermes_data.tag_info:
            buckets.append(self.translate_buckets(bucket))
        for blob in self.hermes_data.blob_info:
            blobs.append(self.translate_blobs(blob))
        for target in self.hermes_data.tgt_info:
            targets.append(self.translate_targets(target))

        combined_hash = hashlib.sha256((self.compute_hash(buckets) + self.compute_hash(blobs) +
                                        self.compute_hash(targets)).encode()).hexdigest()

        self.translated = {
            "id": combined_hash,
            "buckets": buckets,
            "blobs": blobs,
            "targets": targets
        }

    def translate_blob(self, blob):
        buffer_info = {
            "target_id": target_id,  # Int
            "size": size  # Int
        }

        return {
            "id": blob_id,  # int
            "name": name,  # String
            "score": score,  # int
            "access_frequency": access_frequency,  # float
            "buffer_info": buffer_info
        }

    def translate_target(self, target):
        return {
            "name": target_type,  # The type NVME, Memory, as a string
            "id": target_id,  # A number
            "remaining_capacity": remaining_capacity,  # Float
            "node_id": node_id  # String. ex=ares-comp-01
        }

    def translate_bucket(self, bucket):
        return {
            "name": name,  # A string with the bucker name
            "id": bucket_id,  # An integer
            "traits": traits,  # I made it a list of strings, but i dont think i am using it at the moment
            "blobs": blob_ids  # A list of unique random integers
        }

    def compute_hash(self, d):
        json_str = json.dumps(d, sort_keys=True)
        hash_obj = hashlib.sha256(json_str.encode())
        return hash_obj.hexdigest()

    def get_traslation(self):
        return self.translated
