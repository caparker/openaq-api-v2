import logging
import time
import os

import asyncpg
from .models.auth import User
import orjson
from aiocache import SimpleMemoryCache, cached
from aiocache.plugins import HitMissRatioPlugin, TimingPlugin
from buildpg import render
from fastapi import HTTPException, Request
from asyncio.exceptions import TimeoutError

from openaq_api.settings import settings

from .models.responses import Meta, OpenAQResult

logger = logging.getLogger("db")


def default(obj):
    return str(obj)


def dbkey(m, f, query, args):
    j = orjson.dumps(
        args, option=orjson.OPT_OMIT_MICROSECONDS, default=default
    ).decode()
    dbkey = f"{query}{j}"
    h = hash(dbkey)
    # logger.debug(f"dbkey: {dbkey} h: {h}")
    return h


cache_config = {
    "key_builder": dbkey,
    "cache": SimpleMemoryCache,
    "noself": True,
    "plugins": [
        HitMissRatioPlugin(),
        TimingPlugin(),
    ],
}


async def db_pool(pool):
    # each time we create a connect make sure it can
    # properly convert json/jsonb fields
    async def init(con):
        await con.set_type_codec(
            "jsonb", encoder=orjson.dumps, decoder=orjson.loads, schema="pg_catalog"
        )
        await con.set_type_codec(
            "json", encoder=orjson.dumps, decoder=orjson.loads, schema="pg_catalog"
        )

    logger.debug(f"Checking for existing pool: {pool}")
    if pool is None:
        logger.debug("Creating a new pool")
        pool = await asyncpg.create_pool(
            settings.DATABASE_READ_URL,
            command_timeout=6,
            max_inactive_connection_lifetime=15,
            min_size=1,
            max_size=10,
            init=init,
        )
    return pool


class DB:
    def __init__(self, request: Request):
        self.request = request
        logger.debug(f"New db: {request.app.state}")

    async def acquire(self):
        pool = await self.pool()
        return pool

    async def pool(self):
        self.request.app.state.pool = await db_pool(
            getattr(self.request.app.state, "pool", None)
        )
        return self.request.app.state.pool

    @cached(settings.API_CACHE_TIMEOUT, **cache_config)
    async def fetch(self, query, kwargs):
        pool = await self.pool()
        start = time.time()
        logger.debug("Start time: %s\nQuery: %s \nArgs:%s\n", start, query, kwargs)
        rquery, args = render(query, **kwargs)
        async with pool.acquire() as con:
            try:
                r = await con.fetch(rquery, *args)
            except asyncpg.exceptions.UndefinedColumnError as e:
                logger.error(f"Undefined Column Error: {e}\n{rquery}\n{kwargs}")
                raise ValueError(f"{e}") from e
            except asyncpg.exceptions.CharacterNotInRepertoireError as e:
                raise ValueError(f"{e}") from e
            except asyncpg.exceptions.DataError as e:
                logger.error(f"Data Error: {e}\n{rquery}\n{kwargs}")
                raise ValueError(f"{e}") from e
            except TimeoutError:
                raise HTTPException(
                    status_code=408,
                    detail="Connection timed out",
                )
            except Exception as e:
                logger.error(f"Unknown database error: {e}\n{rquery}\n{kwargs}")
                if str(e).startswith("ST_TileEnvelope"):
                    raise HTTPException(status_code=422, detail=f"{e}")
                raise HTTPException(status_code=500, detail=f"{e}")
        logger.debug(
            "query took: %s and returned:%s\n -- results_firstrow: %s",
            time.time() - start,
            len(r),
            str(r and r[0])[0:1000],
        )
        return r

    async def fetchrow(self, query, kwargs):
        r = await self.fetch(query, kwargs)
        if len(r) > 0:
            return r[0]
        return []

    async def fetchval(self, query, kwargs):
        r = await self.fetchrow(query, kwargs)
        if len(r) > 0:
            return r[0]
        return None

    async def fetchPage(self, query, kwargs) -> OpenAQResult:
        page = kwargs.get("page", 1)
        limit = kwargs.get("limit", 1000)
        kwargs["offset"] = abs((page - 1) * limit)

        data = await self.fetch(query, kwargs)
        if len(data) > 0:
            if "found" in data[0].keys():
                kwargs["found"] = data[0]["found"]
            elif len(data) == limit:
                kwargs["found"] = f">{limit}"
            else:
                kwargs["found"] = len(data)
        else:
            kwargs["found"] = 0

        output = OpenAQResult(
            meta=Meta.model_validate(kwargs), results=[dict(x) for x in data]
        )
        return output

    async def create_user(self, user: User) -> str:
        """
        calls the create_user plpgsql function to create a new user and entity records
        """
        query = """
        SELECT * FROM create_user(:full_name, :email_address, :password_hash, :ip_address, :entity_type)
        """
        conn = await asyncpg.connect(settings.DATABASE_WRITE_URL)
        rquery, args = render(query, **user.model_dump())
        verification_token = await conn.fetch(rquery, *args)
        await conn.close()
        return verification_token[0][0]

    async def get_user_token(self, users_id: int) -> str:
        """
        calls the get_user_token plpgsql function to vefiry user email and generate API token
        """
        query = """
        SELECT * FROM get_user_token(:users_id)
        """
        conn = await asyncpg.connect(settings.DATABASE_WRITE_URL)
        rquery, args = render(query, **{"users_id": users_id})
        api_token = await conn.fetch(rquery, *args)
        await conn.close()
        return api_token[0][0]

    async def fetchOpenAQResult(self, query, kwargs):
        rows = await self.fetch(query, kwargs)
        found = 0
        results = []

        if len(rows) > 0:
            if "count" in rows[0].keys():
                found = rows[0]["count"]
            # OpenAQResult expects a list for results
            if rows[0][1] is not None:
                if isinstance(rows[0][1], list):
                    results = rows[0][1]
                elif isinstance(rows[0][1], dict):
                    results = [r[1] for r in rows]
                elif isinstance(rows[0][1], str):
                    results = [r[1] for r in rows]

        meta = Meta(
            website=os.getenv("DOMAIN_NAME", os.getenv("BASE_URL", "/")),
            page=kwargs["page"],
            limit=kwargs["limit"],
            found=found,
        )
        output = OpenAQResult(meta=meta, results=results)
        return output
