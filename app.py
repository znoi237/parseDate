import os
import logging
from flask import Flask, redirect, url_for
from config import configure_logging, Config
from database import DatabaseManager
from api import api_bp, make_services
from pages import pages_bp

def create_app():
    configure_logging()
    logger = logging.getLogger("ai_trader")

    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config.from_object(Config)

    # Core services (db, data managers, model manager, ws, news, bots)
    services = make_services(app)
    app.extensions["services"] = services

    # Blueprints
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(pages_bp)

    @app.route("/")
    def index():
        return redirect(url_for("pages.dashboard"))

    logger.info("App created and blueprints registered")
    return app

if __name__ == "__main__":
    app = create_app()
    host = os.environ.get("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.environ.get("FLASK_RUN_PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "0").lower() in ("1","true","yes")
    app.run(host=host, port=port, debug=debug, threaded=True)