import ujson
import asyncpg
from functools import partial


class DB:

    def __init__(self):
        self.pool = None

    async def init(self, *args, **kwds):
        self.pool = await asyncpg.create_pool(*args, init=self.codecs, **kwds)

    def __getattr__(self, item):
        return getattr(self.pool, item)

    @staticmethod
    async def codecs(conn: asyncpg.Connection):
        await conn.set_type_codec(
            'json',
            encoder=partial(ujson.dumps, ensure_ascii=False),
            decoder=ujson.loads,
            schema='pg_catalog',
            format='text'
        )


db = DB()
