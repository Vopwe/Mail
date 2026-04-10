"""
Flask app factory.
"""
from flask import Flask
import database


def create_app() -> Flask:
    app = Flask(
        __name__,
        static_folder="static",
        template_folder="templates",
    )
    app.secret_key = "email-extractor-local-dev-key"

    database.init_db()

    from web.routes.dashboard import bp as dashboard_bp
    from web.routes.campaigns import bp as campaigns_bp
    from web.routes.emails import bp as emails_bp
    from web.routes.verification import bp as verification_bp
    from web.routes.settings import bp as settings_bp
    from web.routes.api import bp as api_bp

    app.register_blueprint(dashboard_bp)
    app.register_blueprint(campaigns_bp, url_prefix="/campaigns")
    app.register_blueprint(emails_bp, url_prefix="/emails")
    app.register_blueprint(verification_bp, url_prefix="/verification")
    app.register_blueprint(settings_bp, url_prefix="/settings")
    app.register_blueprint(api_bp, url_prefix="/api")
    app.teardown_appcontext(lambda _exc: database.close_db())

    return app
