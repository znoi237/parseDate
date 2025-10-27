from flask import Blueprint, render_template

pages_bp = Blueprint("pages", __name__)

@pages_bp.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")

@pages_bp.route("/training")
def training():
    return render_template("training.html")

@pages_bp.route("/settings/api")
def settings_api():
    return render_template("settings_api.html")

@pages_bp.route("/settings/indicators")
def settings_indicators():
    return render_template("settings_indicators.html")

@pages_bp.route("/settings/trading")
def settings_trading():
    return render_template("settings_trading.html")

@pages_bp.route("/bots")
def bots():
    return render_template("bots.html")

@pages_bp.route("/symbol")
def symbol():
    return render_template("symbol.html")