"""
db.py – Database connection pool and schema initialization.
"""

from __future__ import annotations

import asyncpg
from pathlib import Path

_pool: asyncpg.Pool | None = None


async def init_db(database_url: str) -> None:
    """Initialize connection pool and apply schema."""
    global _pool
    _pool = await asyncpg.create_pool(database_url, min_size=2, max_size=10)
    
    # Apply schema idempotently
    schema_path = Path(__file__).parent.parent / "db" / "schema.sql"
    with open(schema_path, encoding="utf-8") as f:
        schema_sql = f.read()
    
    async with _pool.acquire() as conn:
        await conn.execute(schema_sql)


async def close_db() -> None:
    """Close connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    """Return the connection pool."""
    if _pool is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _pool
