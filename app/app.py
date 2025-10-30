# app/app.py - Flask application factory

import os
import logging
from flask import Flask

from .controllers.main_controller import main_bp
from .models.db import SECRET_KEY, SUPABASE_URL, GTM_ID, PER_PAGE_MAX, close_db, init_db, logger, ANALYTICS_SALT

# ------------------------- Application Factory -------------------------------

def create_app():
    """Create and configure Flask application."""
    app = Flask(__name__, template_folder="views/templates")
    env = os.getenv("FLASK_ENV") or os.getenv("ENV") or "production"
    if not SUPABASE_URL:
        logger.error("SUPABASE_URL (or DATABASE_URL) must be configured before starting the app.")
        raise SystemExit(1)

    app.config.update(
        SECRET_KEY=SECRET_KEY,
        GTM_CONTAINER_ID=GTM_ID,
        # Reload templates only outside production
        TEMPLATES_AUTO_RELOAD=(env != "production"),
        PER_PAGE_MAX=PER_PAGE_MAX,
        SUPABASE_URL=SUPABASE_URL,
    )

    # Register blueprints
    app.register_blueprint(main_bp)
    # Initialize rate limiting if present
    try:
        from .controllers.main_controller import limiter as _limiter
        if _limiter:
            _limiter.init_app(app)
    except Exception:
        pass

    # Register database teardown
    app.teardown_appcontext(close_db)

    # Enforce SECRET_KEY always (safer default)
    if not SECRET_KEY or SECRET_KEY == "dev-insecure-change-me":
        logger.error("SECRET_KEY must be set via environment. Aborting.")
        raise SystemExit(1)

    # Warn if analytics salt is default in production
    if env == "production" and (not ANALYTICS_SALT or ANALYTICS_SALT.strip().lower() == "dev"):
        logger.warning("ANALYTICS_SALT is using a default value in production; set ANALYTICS_SALT to a unique secret.")

    # Ensure DB schema exists (including Jobs table)
    try:
        with app.app_context():
            init_db()
    except Exception as e:
        logger.warning("init_db failed: %s", e)

    return app

# ------------------------- Main Entry Point ----------------------------------
if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
