from dagster import asset
from pandas import DataFrame
from pandas.io.json import json_normalize
import asyncio
import aiohttp
from bs4 import BeautifulSoup

from ...api_puller.read_api import *

pg_user = os.environ.get("pg_user")
pg_password = os.environ.get("pg_password")
pg_host = os.environ.get("pg_host")
pg_port = os.environ.get("pg_port")
pg_db = os.environ.get("pg_db")
write_engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

@asset
def articles_prod() -> DataFrame:
    conn_articles = ConnectToPostgres("articles", pg_user, pg_password, pg_host, pg_port, pg_db)
    max_id_articles = conn_articles.get_max_data_id()
    spacedevs_api_articles = SpacedevsAPI(type = "articles", max_id=max_id_articles, _limit=1000000)
    articles = spacedevs_api_articles.get_data()
    articles.to_sql('articles', con = write_engine, index = False, if_exists = 'append', schema = 'spacedevs')
    return articles

@asset
def blogs_prod() -> DataFrame:
    conn_blogs = ConnectToPostgres("blogs", pg_user, pg_password, pg_host, pg_port, pg_db)
    max_id_blogs = conn_blogs.get_max_data_id()
    spacedevs_api_blogs = SpacedevsAPI(type = "blogs", max_id=max_id_blogs, _limit=1000000)
    blogs = spacedevs_api_blogs.get_data()
    blogs.to_sql('blogs', con = write_engine, index = False, if_exists = 'append', schema = 'spacedevs')
    return blogs

@asset
def reports_prod() -> DataFrame:
    conn_reports = ConnectToPostgres("reports", pg_user, pg_password, pg_host, pg_port, pg_db)
    max_id_reports = conn_reports.get_max_data_id()
    spacedevs_api_reports = SpacedevsAPI(type = "reports", max_id=max_id_reports, _limit=1000000)
    reports = spacedevs_api_reports.get_data()
    reports.to_sql('reports', con = write_engine, index = False, if_exists = 'append', schema = 'spacedevs')
    return reports

@asset
def posts_prod(articles_prod, blogs_prod, reports_prod):
    pg_user = os.environ.get("pg_user")
    pg_password = os.environ.get("pg_password")
    pg_host = os.environ.get("pg_host")
    pg_port = os.environ.get("pg_port")
    pg_db = os.environ.get("pg_db")

    write_engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")
    union_df = pd.read_sql(union_statement, write_engine)
    async def get(url, pkey, title, published_at, published_date, image_url, api_source, news_site, summary, launch_name, launch_slug, event_name, event_slug, session):
        try:
            async with session.get(url=url) as response:
                print(f"Reading: {title}")
                html_doc = await response.read()
                soup = BeautifulSoup(html_doc, 'html.parser')
                body = ''
                for p in soup.find_all('p'):
                    body += str(p)
                union_df.loc[union_df['id'] == pkey, 'content'] = body
        except:
            pass

    async def main(df):
        async with aiohttp.ClientSession() as session:
            ret = await asyncio.gather(*[get(url=row["url"], pkey=row["id"], title=row["title"], published_at=row["published_at"], published_date=row["published_date"], image_url=row["image_url"], api_source=row["api_source"], news_site=row["news_site"], summary=row["summary"], launch_name=row['launch_name'], launch_slug=row['launch_slug'], event_name=row['event_name'], event_slug=row['event_slug'], session=session) for index, row in union_df.iterrows()])
        return ret

    asyncio.run(main(union_df))
    union_df.to_sql('posts', con = write_engine, index = False, if_exists = 'append', schema = 'spacedevs')