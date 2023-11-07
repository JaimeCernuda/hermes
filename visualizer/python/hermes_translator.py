import json

from py_hermes import Hermes, TRANSPARENT_HERMES
import hashlib
from jarvis_util import Hostfile

class MetadataSnapshot:
    def __init__(self, hostfile):
        TRANSPARENT_HERMES()
        self.hermes = Hermes()
        self.blob_info = []
        self.target_info = []
        self.tag_info = []
        self.target_types = ["Memory", "NVMe", "BB", "Pfs"]
        self.hosts = Hostfile(hostfile=hostfile).hosts

    @staticmethod
    def unique(id):
        return f'{id.node_id}.{id.unique}'

    def collect(self):
        mdm = self.hermes.CollectMetadataSnapshot()
        tag_to_blob = {}
        tid_to_tgt = {}
        for target in mdm.target_info:
            target_info = {
                'name': None,
                'id':  self.unique(target.tgt_id),
                'node_id': int(target.node_id),
                'rem_cap': target.rem_cap,
                'max_cap': target.max_cap,
                'bandwidth': target.bandwidth,
                'latency': target.latency,
                'score': target.score,
            }
            self.target_info.append(target_info)
            tid_to_tgt[target_info['id']] = target_info
        self.target_info.sort(reverse=True, key=lambda x: x['bandwidth'])
        for i, target in enumerate(self.target_info):
            target['name'] = f'Tier {i}'
        for blob in mdm.blob_info:
            blob_info = {
                'name': str(blob.get_name()),
                'id': self.unique(blob.blob_id),
                'mdm_node': int(blob.blob_id.node_id),
                'tag_id': self.unique(blob.tag_id),
                'score': float(blob.score),
                'access_frequency': 0,
                'buffer_info': []
            }
            for buf in blob.buffers:
                buf_info = {
                    'target_id': self.unique(buf.tid),
                    'node_id': 0,
                    'size': int(buf.t_size)
                }
                buf_info['node_id'] = tid_to_tgt[buf_info['target_id']]['node_id']
                blob_info['buffer_info'].append(buf_info)
            self.blob_info.append(blob_info)
            if blob_info['tag_id'] not in tag_to_blob:
                tag_to_blob[blob_info['tag_id']] = []
            tag_to_blob[blob_info['tag_id']].append(blob_info['id'])
        for tag in mdm.bkt_info:
            tag_info = {
                'id': self.unique(tag.tag_id),
                'mdm_node': int(tag.tag_id.node_id),
                'name': str(tag.get_name()),
                # 'blobs': [self.unique(blob.blob_id) for blob in tag.blobs]
                'blobs': []
            }
            if tag_info['id'] in tag_to_blob:
                tag_info['blobs'] = tag_to_blob[tag_info['id']]
            self.tag_info.append(tag_info)

    def compute_hash(self, d):
        json_str = json.dumps(d, sort_keys=True)
        hash_obj = hashlib.sha256(json_str.encode())
        return hash_obj.hexdigest()

    def generate_metadata(self):
        self.collect()
        targets = self.transform_targets()
        buckets = self.transform_tag_info()
        blobs = self.transform_blob_info()

        combined_hash = hashlib.sha256((self.compute_hash(buckets) +
                                        self.compute_hash(blobs) +
                                        self.compute_hash(targets)).encode()).hexdigest()

        return {
            "id": combined_hash,
            "buckets": buckets,
            "blobs": blobs,
            "targets": targets
        }

    def transform_targets(self):
        transformed_targets = []

        # Group by node_id to ensure we can rank by bandwidth
        targets_by_node = {}
        for target in self.target_info:
            node_index = target['node_id']
            if node_index not in targets_by_node:
                targets_by_node[node_index] = []
            targets_by_node[node_index].append(target)

        # Sort each node's targets by bandwidth and assign target types
        for node_index, targets in targets_by_node.items():
            targets.sort(key=lambda x: x['bandwidth'], reverse=True)
            for i, target in enumerate(targets):
                target_type = self.target_types[i] if i < len(self.target_types) else None
                if target_type:
                    transformed_target = self.transform_single_target(target, node_index, target_type)
                    transformed_targets.append(transformed_target)

        return transformed_targets

    def transform_single_target(self, original, node_index, target_type):
        # Convert the original target to the transformed structure
        transformed = {
            "name": target_type,
            "id": int(original['id'].split('.')[1]),
            "remaining_capacity": round(original['rem_cap'] / original['max_cap'], 2),
            "node_id": self.hosts[node_index - 1]
        }
        return transformed

    def transform_tag_info(self):
        transformed_tags = []

        # Iterate over the original tag info and transform
        for tag in self.tag_info:
            transformed_tag = {
                "name": tag["name"],
                "id": int(tag["id"].split('.')[1]),  # Convert id to int after the dot
                "traits": [],
                "blobs": [int(''.join(blob.split('.'))) for blob in tag["blobs"]]
            }
            transformed_tags.append(transformed_tag)

        return transformed_tags

    def transform_blob_info(self):
        transformed_blobs = []

        # Iterate over the original blob info and transform
        for blob in self.blob_info:
            transformed_blob = {
                "id": int(''.join(blob["id"].split('.'))),  # Convert id to int after the dot 1.23 -> 123
                "name": blob["name"],  # Keep the name the same
                "score": float(blob["score"]),  # Keep the score as float
                "access_frequency": float(blob["access_frequency"]),  # Convert access frequency to float
                "buffer_info": {
                    "target_id": int(blob["buffer_info"][0]["target_id"].split('.')[1]),
                    "size": blob["buffer_info"][0]["size"]
                },
                "bucket_id": int(blob["tag_id"].split('.')[1]),
                "mdm_node": self.hosts[blob["mdm_node"] - 1]  # Assuming mdm_node starts at 1
            }
            transformed_blobs.append(transformed_blob)

        return transformed_blobs
# mdm = MetadataSnapshot()
# mdm.collect()
# print('Done')