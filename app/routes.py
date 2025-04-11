from app.models import Aggregate_data
from app import db
from flask import render_template


def init_app(app):

    @app.route("/")
    def principal():
        return render_template("index.html")
