import time

from flask import Flask, render_template, jsonify, url_for
from python.generator import generate_metadata
from python.hermes_translator import MetadataSnapshot
from python.sql_translator import SQLParser
import argparse
import os

def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, 'r')  # return an open file handle

parser = argparse.ArgumentParser(description="Run a Flask application")
parser.add_argument('--port', type=int, default=5000, help='Port to run the Flask application on')
parser.add_argument('--sleep_time', type=float, default=0.5, help='Sleep time for the /metadata route')
parser.add_argument('--real', type=bool, default=False, help='Generate data or capture form hermes')
parser.add_argument("--hostfile", dest="hostfile", required=True,
                    help="hostfile with nodes under which we are running", metavar="FILE",
                    type=lambda x: is_valid_file(parser, x))
parser.add_argument("--db_path", dest="db_path",
                    help="path to the database to gather the metadata", metavar="FILE",
                    type=lambda x: is_valid_file(parser, x))

args = parser.parse_args()

app = Flask(__name__)

@app.route('/')
def index():
    image_url1 = url_for('static', filename='assets/grc.png')
    image_url2 = url_for('static', filename='assets/grc.jpeg')
    return render_template('index.html', image_url1=image_url1, image_url2=image_url2)

@app.route('/empress_metadata')
def get_metadata():
    time.sleep(args.sleep_time)
    if args.real:
        mdm = SQLParser(args.db_path, args.hostfile)
        metadata = mdm.get_transformed_data()
    else:
        metadata = []
    return jsonify(metadata)

@app.route('/metadata')
def get_metadata():
    time.sleep(args.sleep_time)
    if args.real:
        mdm = MetadataSnapshot(args.hostfile)
        metadata = mdm.generate_metadata()
    else:
        metadata = generate_metadata(num_buckets=3, num_blobs=100, num_targets=4, num_nodes=16)
    return jsonify(metadata)

if __name__ == '__main__':
    app.run(port=args.port, debug=True)
