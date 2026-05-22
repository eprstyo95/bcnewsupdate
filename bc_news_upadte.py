#!/usr/bin/env python3
"""Compatibility shim for the historic misspelled entrypoint.

Use bc_news_update.py for new automation. This file remains so any old cron or
manual command with the typo still runs the real app instead of doing nothing.
"""

from bc_news_update import main


if __name__ == "__main__":
    main()
