import time

from flask import Flask, render_template, jsonify, url_for
import random
from python.generator import generate_metadata
import logging

app = Flask(__name__)

@app.route('/')
def index():
    image_url1 = url_for('static', filename='assets/grc.png')
    image_url2 = url_for('static', filename='assets/grc.jpeg')
    return render_template('index.html', image_url1=image_url1, image_url2=image_url2)


@app.route('/metadata')
def get_metadata():
    time.sleep(0.5)
    metadata = generate_metadata(num_buckets=3, num_blobs=100, num_targets=4, num_nodes=16)
    return jsonify(metadata)

# @app.route('/assets')
# def index():
#     grc_png = url_for('static', filename='assets/grc.png')
#     grc_jpeg = url_for('static', filename='assets/grc.jpeg')
#     return render_template('index.html', image_url1=grc_png, image_url2=grc_jpeg)

if __name__ == '__main__':
    app.run(debug=True)
