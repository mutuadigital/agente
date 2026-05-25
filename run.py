#!/usr/bin/env python3
import os
from dotenv import load_dotenv

load_dotenv()

from support_agent.database import init_db
init_db()

from support_agent.app import app

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    workers = int(os.getenv("WORKERS", 2))

    import gunicorn.app.base

    class StandaloneApp(gunicorn.app.base.BaseApplication):
        def __init__(self, application, options=None):
            self.options = options or {}
            self.application = application
            super().__init__()

        def load_config(self):
            for key, value in self.options.items():
                self.cfg.set(key.lower(), value)

        def load(self):
            return self.application

    options = {
        "bind": f"0.0.0.0:{port}",
        "workers": workers,
        "threads": 4,
        "worker_class": "gthread",
        "timeout": 30,
        "accesslog": "-",
        "errorlog": "-",
    }
    StandaloneApp(app, options).run()
