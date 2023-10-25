from flask import Flask, render_template, jsonify
import random
from python.generator import generate_metadata
import logging

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/metadata')
def get_metadata():
    metadata = generate_metadata(num_buckets=3, num_blobs=8, num_targets=4, num_nodes=32)
    return jsonify(metadata)


if __name__ == '__main__':
    app.run(debug=True)
