from flask import Flask, render_template, jsonify
import random

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def get_data():
    nodes = {}
    for i in range(32):  # Always generate data for 32 nodes
        node_name = 'ares-comp-{0:02d}'.format(i)
        nodes[node_name] = {
            "Memory": random.random(),
            "Nvme": random.random(),
            "BB": random.random(),
            "Pfs": random.random()
        }
    return jsonify(nodes)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3200)
