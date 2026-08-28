"""Test bootstrap for Kurigram on Python versions without a default loop."""

import asyncio


try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())
