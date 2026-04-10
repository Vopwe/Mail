"""
Settings — API keys, crawl config, model selection.
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash
import config

bp = Blueprint("settings", __name__)


@bp.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        updates = {
            "deepseek_api_key": request.form.get("deepseek_api_key", "").strip(),
            "deepseek_model": request.form.get("deepseek_model", "").strip() or config.DEEPSEEK_MODEL,
            "openrouter_api_key": request.form.get("openrouter_api_key", "").strip(),
            "openrouter_model": request.form.get("openrouter_model", "").strip() or config.OPENROUTER_MODEL,
            "ai_concurrency": int(request.form.get("ai_concurrency", 30)),
            "verify_concurrency": int(request.form.get("verify_concurrency", 30)),
            "max_concurrent_requests": int(request.form.get("max_concurrent_requests", 30)),
            "request_timeout": int(request.form.get("request_timeout", 12)),
            "crawl_delay": float(request.form.get("crawl_delay", 0.2)),
            "max_pages_per_domain": int(request.form.get("max_pages_per_domain", 5)),
            "urls_per_batch": int(request.form.get("urls_per_batch", 20)),
            "verify_timeout": int(request.form.get("verify_timeout", 10)),
        }
        config.save_settings(updates)
        flash("Settings saved successfully.", "success")
        return redirect(url_for("settings.index"))

    settings = config.get_all_settings()
    return render_template("settings.html", settings=settings)
