import threading
import json
import os
import sys

from flask import Flask
from flask.ext.restful import Api, Resource, reqparse, fields, marshal
from flask import request
from flask import make_response
from flask import Response

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)
from lib.remap_utils import RemapException

app = Flask(__name__)
api = Api(app)

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


class AppsListApi(Resource):
    def __init__(self):
        super(AppsListApi, self).__init__()

    def get(self):
        return monitor.list_apps()

class JobsListApi(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('type', type = str, required = True, help = 'No worker type specified', location = 'json')
        self.reqparse.add_argument('app', type = str, required = True, help = 'No app name specified', location = 'json')
        self.reqparse.add_argument('priority', type = int, required = True, help = 'No priority specified', location = 'json')
        self.reqparse.add_argument('parallellism', type = int, required = True, help = 'No parallellism specified', location = 'json')
        super(JobsListApi, self).__init__()

    def get(self):
        return monitor.list_jobs()

    def post(self):
        args = self.reqparse.parse_args()
        try:
            results = monitor.start_job( request.json )
            # Created
            return results, 201
        except RemapException as re:
            return str(re), 400

    def delete(self):
        monitor.cancel_job()

class NodesListApi(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('priority', type = int, required = True, help = 'No priority specified', location = 'json')
        super(NodesListApi, self).__init__()

    def get(self):
        return monitor.list_nodes()

    def post(self):
        args = self.reqparse.parse_args()
        monitor.refresh_nodes( args["priority"] )
        return "", 202

class JobApi(Resource):
    def __init__(self):
        #self.reqparse = reqparse.RequestParser()
        #self.reqparse.add_argument('jobid', type=str, location='json')
        super(JobApi, self).__init__()

    def delete(self, id):
        if not monitor.has_job( id ):
            abort(404)


api.add_resource(AppsListApi, '/api/v1.0/apps', endpoint = 'apps')
api.add_resource(JobsListApi, '/api/v1.0/jobs', endpoint = 'jobs')
api.add_resource(NodesListApi, '/api/v1.0/nodes', endpoint = 'nodes')
api.add_resource(JobApi, '/api/v1.0/jobs/<string:id>', endpoint='job')

def run():
    global app
    app.run()

def start( initiator ):
    global monitor
    monitor = initiator
    t = threading.Thread(target=run, args =())
    t.daemon = True
    t.start()

