from jarvis_util import *
import json
from collections import defaultdict

class SQLParser:
    def __init__(self, db_path, hostfile):
        self.db_path = db_path
        self.hostfile = hostfile

    def exec_on_nodes(self):
        #cmd = f'sqlite3 {self.db_path} \'SELECT step, variable, operation, blob_name, bucket_name, value FROM derived_targets;\''
        #cmd = f'sqlite3 -help'
        cmd = f'/home/jcernudagarcia/jarvis-jaime/jarvis-util/bin/query.sh {self.db_path}'

        print(cmd)
        pssh_info = PsshExecInfo(hostfile=self.hostfile, collect_output=True)
        exec_result = Exec(cmd, pssh_info)
        return exec_result

    def fetch_data_from_nodes(self):
        node_outputs = self.exec_on_nodes()
        all_data = {}
        errors = {}
        for node_name, node_output in node_outputs.stdout.items():
            if "Error: Database file not found" in node_output:
                errors[node_name] = "Database file not found."
            elif "Parse error" in node_output and "no such table" in node_output:
                errors[node_name] = "Table does not exist in the database."
            elif not node_output.strip():
                errors[node_name] = "Table exists but is empty."
            else:
                all_data[node_name] = self.parse_node_output(node_output)
        return all_data

    def parse_node_output(self, output):
        rows = [line.split('|') for line in output.strip().split('\n')]
        return rows

    def transform_data(self, data):
        output = {}
        for row in data:
            step, variable, operation, blob_name, bucket_name, value = row
            step = int(step)  # convert step to int
            value = float(value)  # convert value to float

            if step not in output:
                output[step] = {}

            if variable not in output[step]:
                output[step][variable] = {}

            output[step][variable][operation] = {
                'blob': blob_name,
                'bucket': bucket_name,
                'value': value
            }
        return output

    def get_transformed_data(self):
        raw_data = self.fetch_data_from_nodes()
        if not raw_data:
            return {}
        transformed_data = {node: self.transform_data(rows) for node, rows in raw_data.items()}
        return self.append_global_results(transformed_data)

    def append_global_results(self, transformed_data):
        global_results = {}
        # Iterate over each step in each node to gather global min and max
        for node_id, steps in transformed_data.items():
            for step, variables in steps.items():
                if step not in global_results:
                    global_results[step] = {}
                for var_name, var_data in variables.items():
                    if var_name not in global_results[step]:
                        global_results[step][var_name] = {}
                    for op_name, op_data in var_data.items():
                        value = op_data['value']
                        # Initialize if not already
                        if op_name not in global_results[step][var_name]:
                            base_blob = {
                                    'blob': var_data[op_name]['blob'],
                                    'bucket': var_data[op_name]['bucket'],
                                    'value': value
                                }
                            global_results[step][var_name][op_name] = base_blob
                        else:
                            # Aggregate the global min and max
                            if op_name == 'min':
                                if global_results[step][var_name][op_name]['value']  > value:
                                    global_results[step][var_name] = {
                                        op_name: {
                                            'blob': var_data[op_name]['blob'],
                                            'bucket': var_data[op_name]['bucket'],
                                            'value': value
                                        }
                                    }
                            elif op_name == 'max':
                                if global_results[step][var_name][op_name]['value'] < value:
                                    global_results[step][var_name] = {
                                        op_name: {
                                            'blob': var_data[op_name]['blob'],
                                            'bucket': var_data[op_name]['bucket'],
                                            'value': value
                                        }
                                    }

        # Append the global results to transformed_data
        transformed_data['global'] = global_results
        return transformed_data