import time

from flask import Flask, render_template, jsonify, url_for
import random
from python.generator import generate_metadata
import logging
import argparse

parser = argparse.ArgumentParser(description="Run a Flask application")
parser.add_argument('--port', type=int, default=5000, help='Port to run the Flask application on')
parser.add_argument('--sleep_time', type=float, default=0.5, help='Sleep time for the /metadata route')
args = parser.parse_args()

app = Flask(__name__)

@app.route('/')
def index():
    image_url1 = url_for('static', filename='assets/grc.png')
    image_url2 = url_for('static', filename='assets/grc.jpeg')
    return render_template('index.html', image_url1=image_url1, image_url2=image_url2)


@app.route('/metadata')
def get_metadata():
    time.sleep(args.sleep_time)
    metadata = generate_metadata(num_buckets=3, num_blobs=100, num_targets=4, num_nodes=16)
    return jsonify(metadata)

# @app.route('/assets')
# def index():
#     grc_png = url_for('static', filename='assets/grc.png')
#     grc_jpeg = url_for('static', filename='assets/grc.jpeg')
#     return render_template('index.html', image_url1=grc_png, image_url2=grc_jpeg)

if __name__ == '__main__':
    app.run(port=args.port, debug=True)
