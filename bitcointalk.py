import asyncio
import aiohttp
import pendulum
import yaml
from lxml import html
from db import db
from click import secho
from telegram import TelegramApi


config = yaml.load(open('config.yml'))
telegram = TelegramApi(config['telegram']['key'])
sql = '''
create table if not exists forum (
    url text primary key,
    title text,
    replies bigint,
    views bigint,
    created timestamptz
);
alter table forum add column if not exists updated timestamptz;
alter table forum add column if not exists posted boolean default false;
'''


async def fetch(url):
    async with config['session'].get(url) as resp:
        return await resp.read()


async def parse_page():
    url = 'https://bitcointalk.org/index.php?board=159.0'
    print(f'parsing page: {url}')
    data = await fetch(url)
    h = html.fromstring(data)
    for a in h.xpath('//span[starts-with(@id, "msg_")]/a'):
        replies, views = [int(t.strip()) for t in a.xpath('../../following-sibling::td/text()')[2:4]]
        yield {
            'url': a.attrib['href'],
            'title': a.text,
            'replies': replies,
            'views': views,
        }


async def parse_topic(url):
    print(f'parsing topic: {url}')
    data = await fetch(url)
    h = html.fromstring(data)
    text = h.xpath('//div[@class="subject"]/following-sibling::div')[0].text_content()
    text = text.replace('Today at', pendulum.utcnow().to_date_string())
    created = pendulum.parse(text)
    await db.execute(
        'update forum set created = $2 where url = $1',
        url, created
    )
    return created


async def notify(topic):
    url = topic['url']
    title = topic['title']
    replies = topic['replies']
    views = topic['views']
    created = pendulum.instance(topic['created']).to_datetime_string()

    text = f'{title}\nreplies: {replies}, views: {views}, created: {created}\n{url}'
    secho(text, fg='green')
    telegram.send_message(chat_id=config['telegram']['chat_id'], text=text)
    await db.execute('update forum set posted = true where url = $1', url)


async def fetch_topic(url):
    topic = await db.fetchrow('select * from forum where url = $1', url)
    if topic:
        return dict(topic)


async def insert_topic(topic):
    return await db.fetchval(
        '''insert into forum (url, title, replies, views, updated) values ($1, $2, $3, $4, now())
        on conflict (url) do update set title = $2, replies = $3, views = $4, updated = now()
        returning url''',
        topic['url'], topic['title'], topic['replies'], topic['views']
    )


async def detect_new():
    async for topic in parse_page():
        url = topic['url']
        await insert_topic(topic)
        topic = await fetch_topic(url)
        if not topic['created']:
            topic['created'] = await parse_topic(url)
        if not topic['posted'] and meets_criteria(topic):
            await notify(topic)


def meets_criteria(topic):
    ignore_older = pendulum.parse(config['bitcointalk']['ignore_older'])
    return (topic['created'] > ignore_older and
            topic['views'] >= config['bitcointalk']['min_views'] and
            topic['replies'] >= config['bitcointalk']['min_replies'])


async def main():
    print('bitcointalk parser')
    config['session'] = aiohttp.ClientSession()
    await db.init(**config['db'])
    print(await db.execute(sql))
    await detect_new()
    await config['session'].close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
