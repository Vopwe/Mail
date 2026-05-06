"""
Settings — API keys, crawl config, search config, model selection, password.
"""
from flask import Blueprint, render_template, request, redirect, url_for, flash
import config
from web.auth import get_app_password, set_app_password, check_password

bp = Blueprint("settings", __name__)


@bp.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        updates = {
            # AI providers (legacy DeepSeek kept for backward compat)
            "deepseek_api_key": request.form.get("deepseek_api_key", "").strip(),
            "deepseek_model": request.form.get("deepseek_model", "").strip() or config.DEEPSEEK_MODEL,
            "openrouter_api_key": request.form.get("openrouter_api_key", "").strip(),
            "openrouter_model": request.form.get("openrouter_model", "").strip() or config.OPENROUTER_MODEL,
            # Campaign settings
            "max_running_campaigns": int(request.form.get("max_running_campaigns", config.MAX_RUNNING_CAMPAIGNS)),
            "urls_per_batch": int(request.form.get("urls_per_batch", 40)),
            "url_source_mode": request.form.get("url_source_mode", "both").strip(),
            # Search engine settings
            "bing_delay_min": float(request.form.get("bing_delay_min", config.BING_DELAY_MIN)),
            "bing_delay_max": float(request.form.get("bing_delay_max", config.BING_DELAY_MAX)),
            "bing_results_per_page": int(request.form.get("bing_results_per_page", config.BING_RESULTS_PER_PAGE)),
            "ddg_delay_min": float(request.form.get("ddg_delay_min", config.DDG_DELAY_MIN)),
            "ddg_delay_max": float(request.form.get("ddg_delay_max", config.DDG_DELAY_MAX)),
            # Crawl settings
            "max_concurrent_requests": int(request.form.get("max_concurrent_requests", 30)),
            "request_timeout": int(request.form.get("request_timeout", 12)),
            "crawl_delay": float(request.form.get("crawl_delay", 0.2)),
            "max_pages_per_domain": int(request.form.get("max_pages_per_domain", 5)),
            "robots_txt_mode": request.form.get("robots_txt_mode", "soft").strip(),
            # Verification
            "verify_concurrency": int(request.form.get("verify_concurrency", 30)),
            "verify_timeout": int(request.form.get("verify_timeout", 10)),
        }
        config.save_settings(updates)

        # Handle password change
        new_password = request.form.get("new_password", "").strip()
        if new_password:
            current_pw = request.form.get("current_password", "").strip()
            if get_app_password() and not check_password(current_pw):
                flash("Current password is incorrect. Password not changed.", "error")
            else:
                set_app_password(new_password)
                flash("Settings saved. Password updated.", "success")
                return redirect(url_for("settings.index"))

        # Handle password removal
        remove_pw = request.form.get("remove_password", "")
        if remove_pw == "1":
            config.save_settings({"app_password": "", "app_password_hash": ""})
            flash("Settings saved. Password protection removed.", "success")
            return redirect(url_for("settings.index"))

        flash("Settings saved successfully.", "success")
        return redirect(url_for("settings.index"))

    settings = config.get_all_settings()
    has_password = bool(get_app_password())
    return render_template("settings.html", settings=settings, has_password=has_password)
