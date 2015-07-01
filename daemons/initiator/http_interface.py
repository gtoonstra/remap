import threading
import json
import os
import sys

from flask import Flask
from flask.ext.classy import FlaskView, route
from flask import request
from flask import make_response
from flask import Response

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)
from lib.remap_utils import RemapException

app = Flask(__name__)
monitor = None

@app.errorhandler(400)
def not_found(error):
    app.logger.exception(error)
    resp = make_response(str(error), 400)
    return resp

@app.errorhandler(KeyError)
def key_error(error):
    app.logger.exception(error)
    return make_response(str(error), 400)

@app.errorhandler(RemapException)
def key_error(error):
    app.logger.exception(error)
    return make_response(str(error), 400)

class AppsView(FlaskView):
    def index(self):
        return Response(json.dumps( monitor.list_apps() ),  mimetype='application/json')

class JobsView(FlaskView):
    def index(self):
        return Response(json.dumps( monitor.list_jobs() ),  mimetype='application/json')
    
    @route('/start', methods=["POST"])
    def start(self):
        json_data = request.get_json()
        results = monitor.start_job( json_data )
        return Response(json.dumps(results),  mimetype='application/json')

class NodesView(FlaskView):
    def index(self):
        return Response(json.dumps( monitor.list_nodes() ),  mimetype='application/json')

    @route('/refresh', methods=["POST"])
    def refresh(self):
        json_data = request.get_json()
        monitor.refresh_nodes( json_data["priority"] )
        return Response(json.dumps({}),  mimetype='application/json')

class CoresView(FlaskView):
    def index(self):
        return Response(json.dumps( monitor.list_cores() ),  mimetype='application/json')

AppsView.register(app)
JobsView.register(app)
NodesView.register(app)
CoresView.register(app)

def run():
    global app
    app.run()

def start( initiator ):
    global monitor
    monitor = initiator
    t = threading.Thread(target=run, args =())
    t.daemon = True
    t.start()
