#!/usr/bin/env python3
import os
from dotenv import load_dotenv

load_dotenv()

from support_agent.app import app
from support_agent.database import init_db

if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
