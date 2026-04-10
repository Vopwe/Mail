"""
Dashboard — stats overview + recent campaigns.
"""
from flask import Blueprint, render_template
import database

bp = Blueprint("dashboard", __name__)


@bp.route("/")
def index():
    stats = database.get_stats()
    recent = database.get_campaigns()[:5]
    return render_template("dashboard.html", stats=stats, recent=recent)
