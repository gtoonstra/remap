from flask import Flask
from flask.ext.classy import FlaskView, route

import threading
import json

app = Flask(__name__)
monitor = None

class AppsView(FlaskView):
    def index(self):
        return json.dumps( monitor.list_apps() )

class JobsView(FlaskView):
    def index(self):
        return json.dumps( monitor.list_jobs() )

class NodesView(FlaskView):
    def index(self):
        return json.dumps( monitor.list_nodes() )

    @route('/refresh', methods=["POST"])
    def refresh(self):
        monitor.refresh_nodes()
        return ""

class CoresView(FlaskView):
    def index(self):
        return json.dumps( monitor.list_cores() )

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

