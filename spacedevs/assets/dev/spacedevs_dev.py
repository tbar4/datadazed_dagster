from dagster import asset
from pandas import DataFrame
from pandas.io.json import json_normalize
import asyncio
import aiohttp
from bs4 import BeautifulSoup

from ...api_puller.read_api import *

@asset
def agencies_api_pull_dev():
    agencies = BaseAPI(library="agencies", env = "dev")
    df = agencies.get_api_data()
    agencies.write_to_sink(df=df)
    return df

@asset
def astronaut_dev() -> DataFrame:
    astronauts = BaseAPI(library="astronaut", env = "dev")
    df = astronauts.get_api_data()
    astronauts.write_to_sink(df=df)
    return df

@asset
def config_agencytype_dev() -> DataFrame:
    config_agencytype = BaseAPI(library="config/agencytype", env = "dev")
    df = config_agencytype.get_api_data()
    config_agencytype.write_to_sink(df=df)
    return df

@asset
def config_astronautrole_dev() -> DataFrame:
    config_astronautrole = BaseAPI(library="config/astronautrole", env = "dev")
    df = config_astronautrole.get_api_data()
    config_astronautrole.write_to_sink(df=df)
    return df

@asset
def config_astronautstatus_dev() -> DataFrame:
    config_astronautstatus = BaseAPI(library="config/astronautstatus", env = "dev")
    df = config_astronautstatus.get_api_data()
    config_astronautstatus.write_to_sink(df=df)
    return df

@asset
def config_astronauttype_dev() -> DataFrame:
    config_astronauttype = BaseAPI(library="config/astronauttype", env = "dev")
    df = config_astronauttype.get_api_data()
    config_astronauttype.write_to_sink(df=df)
    return df

@asset
def config_dockinglocation_dev() -> DataFrame:
    config_dockinglocation = BaseAPI(library="config/dockinglocation", env = "dev")
    df = config_dockinglocation.get_api_data()
    config_dockinglocation.write_to_sink(df=df)
    return df

@asset
def config_eventtype_dev() -> DataFrame:
    config_eventtype = BaseAPI(library="config/eventtype", env = "dev")
    df = config_eventtype.get_api_data()
    config_eventtype.write_to_sink(df=df)
    return df

@asset
def config_firststagetype_dev() -> DataFrame:
    config_firststagetype = BaseAPI(library="config/firststagetype", env = "dev")
    df = config_firststagetype.get_api_data()
    config_firststagetype.write_to_sink(df=df)
    return df
    
@asset
def config_landinglocation_dev() -> DataFrame:
    config_landinglocation = BaseAPI(library="config/landinglocation", env = "dev")
    df = config_landinglocation.get_api_data()
    config_landinglocation.write_to_sink(df=df)
    return df

@asset
def config_launcher_dev() -> DataFrame:
    config_launcher = BaseAPI(library="config/launcher", env = "dev")
    df = config_launcher.get_api_data()
    config_launcher.write_to_sink(df=df)
    return df

@asset
def config_launchstatus_dev() -> DataFrame:
    config_launchstatus = BaseAPI(library="config/launchstatus", env = "dev")
    df = config_launchstatus.get_api_data()
    config_launchstatus.write_to_sink(df=df)
    return df

@asset
def config_missiontype_dev() -> DataFrame:
    config_missiontype = BaseAPI(library="config/missiontype", env = "dev")
    df = config_missiontype.get_api_data()
    config_missiontype.write_to_sink(df=df)
    return df

@asset
def config_noticetype_dev() -> DataFrame:
    config_noticetype = BaseAPI(library="config/noticetype", env = "dev")
    df = config_noticetype.get_api_data()
    config_noticetype.write_to_sink(df=df)
    return df

@asset
def config_orbit_dev() -> DataFrame:
    config_orbit = BaseAPI(library="config/orbit", env = "dev")
    df = config_orbit.get_api_data()
    config_orbit.write_to_sink(df=df)
    return df

@asset
def config_roadclosurestatus_dev() -> DataFrame:
    config_roadclosurestatus = BaseAPI(library="config/roadclosurestatus", env = "dev")
    df = config_roadclosurestatus.get_api_data()
    config_roadclosurestatus.write_to_sink(df=df)
    return df

@asset
def config_spacecraft_dev() -> DataFrame:
    config_spacecraft = BaseAPI(library="config/spacecraft", env = "dev")
    df = config_spacecraft.get_api_data()
    config_spacecraft.write_to_sink(df=df)
    return df

@asset
def config_spacecraftstatus_dev() -> DataFrame:
    config_spacecraftstatus = BaseAPI(library="config/spacecraftstatus", env = "dev")
    df = config_spacecraftstatus.get_api_data()
    config_spacecraftstatus.write_to_sink(df=df)
    return df

@asset
def config_spacestationstatus_dev() -> DataFrame:
    config_spacestationstatus = BaseAPI(library="config/spacestationstatus", env = "dev")
    df = config_spacestationstatus.get_api_data()
    config_spacestationstatus.write_to_sink(df=df)
    return df

@asset
def docking_event_dev() -> DataFrame:
    docking_event = BaseAPI(library="docking_event", filter="docking__gt", env = "dev")
    df = docking_event.get_api_data()
    docking_event.write_to_sink(df=df)
    return df

@asset
def event_dev() -> DataFrame:
    event = BaseAPI(library="event", env = "dev")
    df = event.get_api_data()
    df = parse_json_col(df=df, column="launches", new_col_name="launch_id")
    df = parse_json_col(df=df, column="updates", new_col_name="update_id")
    df = parse_json_col(df=df, column="expeditions", new_col_name="expedition_id")
    df = parse_json_col(df=df, column="spacestations", new_col_name="spacestation_id")
    df = parse_json_col(df=df, column="program", new_col_name="program_id")
    event.write_to_sink(df=df)
    return df

@asset
def expedition_dev() -> DataFrame:
    expedition = BaseAPI(library="expedition", env = "dev")
    df = expedition.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="mission_patches", new_col_name="mission_patch_id", drop_col=False)
    df = parse_json_col(df=df, index_label="name", column="mission_patches", new_col_name="mission_patch_name", drop_col=False)
    df = parse_json_col(df=df, index_label="priority", column="mission_patches", new_col_name="mission_priority", drop_col=False)
    df = parse_json_col(df=df, index_label="image_url", column="mission_patches", new_col_name="mission_patch_image_url")
    expedition.write_to_sink(df=df)
    return df

@asset
def launch_dev() -> DataFrame:
    launch = BaseAPI(library="launch", env = "dev")
    df = launch.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="program", new_col_name="program_id")
    launch.write_to_sink(df=df)
    return df

@asset
def launcher_dev() -> DataFrame:
    launcher = BaseAPI(library="launcher", env = "dev")
    df = launcher.get_api_data()
    launcher.write_to_sink(df=df)
    return df

@asset
def location_dev() -> DataFrame:
    location = BaseAPI(library="location", env = "dev")
    df = location.get_api_data()
    location.write_to_sink(df=df)
    return df

@asset
def pad_dev() -> DataFrame:
    pad = BaseAPI(library="pad", env = "dev")
    df = pad.get_api_data()
    pad.write_to_sink(df=df)
    return df

@asset
def program_dev() -> DataFrame:
    program = BaseAPI(library="program", env = "dev")
    df = program.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="agencies", new_col_name="agency_id")
    program.write_to_sink(df=df)
    return df

@asset
def spacecraft_dev() -> DataFrame:
    spacecraft = BaseAPI(library="spacecraft", env = "dev")
    df = spacecraft.get_api_data()
    spacecraft.write_to_sink(df=df)
    return df

@asset
def spacecraft_flight_dev() -> DataFrame:
    spacecraft_flight = BaseAPI(library="spacecraft/flight", env = "dev")
    df = spacecraft_flight.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="launch_program", new_col_name="launch_program_id")
    spacecraft_flight.write_to_sink(df=df)
    return df

@asset
def spacestation_dev() -> DataFrame:
    spacestation = BaseAPI(library="spacestation", env = "dev")
    df = spacestation.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="owners", new_col_name="owner_id")
    spacestation.write_to_sink(df=df)
    return df

@asset
def updates_dev() -> DataFrame:
    updates = BaseAPI(library="updates", env = "dev")
    df = updates.get_api_data()
    updates.write_to_sink(df=df)
    return df
"""
pg_user = os.environ.get("pg_user")
pg_password = os.environ.get("pg_password")
pg_host = os.environ.get("pg_host")
pg_port = os.environ.get("pg_port")
pg_db = os.environ.get("pg_db")

write_engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

@asset
def articles_dev() -> DataFrame:
    conn_articles = ConnectToPostgres("articles", pg_user, pg_password, pg_host, pg_port, pg_db)
    max_id_articles = conn_articles.get_max_data_id_dev()
    spacedevs_api_articles = SpacedevsAPI(type = "articles", max_id=max_id_articles, _limit=1000000)
    articles = spacedevs_api_articles.get_data()
    articles.to_sql('articles', con = write_engine, index = False, if_exists = 'append', schema = 'spacedevs_dev')
    return articles

@asset
def blogs_dev() -> DataFrame:
    conn_blogs = ConnectToPostgres("blogs", pg_user, pg_password, pg_host, pg_port, pg_db)
    max_id_blogs = conn_blogs.get_max_data_id_dev()
    spacedevs_api_blogs = SpacedevsAPI(type = "blogs", max_id=max_id_blogs, _limit=1000000)
    blogs = spacedevs_api_blogs.get_data()
    blogs.to_sql('blogs', con = write_engine, index = False, if_exists = 'append', schema = 'spacedevs_dev')
    return blogs

@asset
def reports_dev() -> DataFrame:
    conn_reports = ConnectToPostgres("reports", pg_user, pg_password, pg_host, pg_port, pg_db)
    max_id_reports = conn_reports.get_max_data_id_dev()
    spacedevs_api_reports = SpacedevsAPI(type = "reports", max_id=max_id_reports, _limit=1000000)
    reports = spacedevs_api_reports.get_data()
    reports.to_sql('reports', con = write_engine, index = False, if_exists = 'append', schema = 'spacedevs_dev')
    return reports

union_df = pd.read_sql(dev_union_statement, write_engine)
async def get(url, pkey, title, published_at, published_date, image_url, api_source, news_site, summary, launch_name, launch_slug, event_name, event_slug, session):
    try:
        async with session.get(url=url) as response:
            print(f"Reading: {title}")
            html_doc = await response.read()
            soup = BeautifulSoup(html_doc, 'html.parser')
            body = ''
            for p in soup.find_all('p'):
                body += str(p)
            df.loc[df['id'] == pkey, 'content'] = body
    except:
        pass

async def main(df):
    async with aiohttp.ClientSession() as session:
        ret = await asyncio.gather(*[get(url=row["url"], pkey=row["id"], title=row["title"], published_at=row["published_at"], published_date=row["published_date"], image_url=row["image_url"], api_source=row["api_source"], news_site=row["news_site"], summary=row["summary"], launch_name=row['launch_name'], launch_slug=row['launch_slug'], event_name=row['event_name'], event_slug=row['event_slug'], session=session) for index, row in df.iterrows()])
    return ret

asyncio.run(main(union_df))
union_df.to_sql('posts', con = write_engine, index = False, if_exists = 'append', schema = 'spacedevs_dev')
"""