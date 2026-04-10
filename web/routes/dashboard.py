"""
Dashboard — stats overview + recent campaigns + charts.
"""
import json
from flask import Blueprint, render_template
import database

bp = Blueprint("dashboard", __name__)


@bp.route("/")
def index():
    stats = database.get_stats()
    recent = database.get_campaigns()[:5]
    chart_data = database.get_chart_data()
    return render_template("dashboard.html", stats=stats, recent=recent,
                           chart_data_json=json.dumps(chart_data))
