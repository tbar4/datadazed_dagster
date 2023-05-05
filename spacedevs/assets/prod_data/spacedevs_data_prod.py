from dagster import asset
from pandas import DataFrame
from pandas.io.json import json_normalize
import asyncio
import aiohttp
from bs4 import BeautifulSoup

from ...api_puller.read_api import *

@asset
def agencies_prod(config_agencytype_prod) -> DataFrame:
    agencies = BaseAPI(library="agencies", env = "prod")
    df = agencies.get_api_data()
    agencies.write_to_sink(df=df)
    return df

@asset
def astronaut_prod(config_astronautrole_prod, config_astronautstatus_prod, config_astronauttype_prod) -> DataFrame:
    astronauts = BaseAPI(library="astronaut", env = "prod")
    df = astronauts.get_api_data()
    astronauts.write_to_sink(df=df)
    return df

@asset
def config_agencytype_prod() -> DataFrame:
    config_agencytype = BaseAPI(library="config/agencytype", env = "prod")
    df = config_agencytype.get_api_data()
    config_agencytype.write_to_sink(df=df)
    return df

@asset
def config_astronautrole_prod() -> DataFrame:
    config_astronautrole = BaseAPI(library="config/astronautrole", env = "prod")
    df = config_astronautrole.get_api_data()
    config_astronautrole.write_to_sink(df=df)
    return df

@asset
def config_astronautstatus_prod() -> DataFrame:
    config_astronautstatus = BaseAPI(library="config/astronautstatus", env = "prod")
    df = config_astronautstatus.get_api_data()
    config_astronautstatus.write_to_sink(df=df)
    return df

@asset
def config_astronauttype_prod() -> DataFrame:
    config_astronauttype = BaseAPI(library="config/astronauttype", env = "prod")
    df = config_astronauttype.get_api_data()
    config_astronauttype.write_to_sink(df=df)
    return df

@asset
def config_dockinglocation_prod() -> DataFrame:
    config_dockinglocation = BaseAPI(library="config/dockinglocation", env = "prod")
    df = config_dockinglocation.get_api_data()
    config_dockinglocation.write_to_sink(df=df)
    return df

@asset
def config_eventtype_prod() -> DataFrame:
    config_eventtype = BaseAPI(library="config/eventtype", env = "prod")
    df = config_eventtype.get_api_data()
    config_eventtype.write_to_sink(df=df)
    return df

@asset
def config_firststagetype_prod() -> DataFrame:
    config_firststagetype = BaseAPI(library="config/firststagetype", env = "prod")
    df = config_firststagetype.get_api_data()
    config_firststagetype.write_to_sink(df=df)
    return df

@asset
def config_landinglocation_prod() -> DataFrame:
    config_landinglocation = BaseAPI(library="config/landinglocation", env = "prod")
    df = config_landinglocation.get_api_data()
    config_landinglocation.write_to_sink(df=df)
    return df

@asset
def config_launcher_prod() -> DataFrame:
    config_launcher = BaseAPI(library="config/launcher", env = "prod")
    df = config_launcher.get_api_data()
    config_launcher.write_to_sink(df=df)
    return df

@asset
def config_launchstatus_prod() -> DataFrame:
    config_launchstatus = BaseAPI(library="config/launchstatus", env = "prod")
    df = config_launchstatus.get_api_data()
    config_launchstatus.write_to_sink(df=df)
    return df

@asset
def config_missiontype_prod() -> DataFrame:
    config_missiontype = BaseAPI(library="config/missiontype", env = "prod")
    df = config_missiontype.get_api_data()
    config_missiontype.write_to_sink(df=df)
    return df

@asset
def config_noticetype_prod() -> DataFrame:
    config_noticetype = BaseAPI(library="config/noticetype", env = "prod")
    df = config_noticetype.get_api_data()
    config_noticetype.write_to_sink(df=df)
    return df

@asset
def config_orbit_prod() -> DataFrame:
    config_orbit = BaseAPI(library="config/orbit", env = "prod")
    df = config_orbit.get_api_data()
    config_orbit.write_to_sink(df=df)
    return df

@asset
def config_roadclosurestatus_prod() -> DataFrame:
    config_roadclosurestatus = BaseAPI(library="config/roadclosurestatus", env = "prod")
    df = config_roadclosurestatus.get_api_data()
    config_roadclosurestatus.write_to_sink(df=df)
    return df

@asset
def config_spacecraft_prod() -> DataFrame:
    config_spacecraft = BaseAPI(library="config/spacecraft", env = "prod")
    df = config_spacecraft.get_api_data()
    config_spacecraft.write_to_sink(df=df)
    return df

@asset
def config_spacecraftstatus_prod() -> DataFrame:
    config_spacecraftstatus = BaseAPI(library="config/spacecraftstatus", env = "prod")
    df = config_spacecraftstatus.get_api_data()
    config_spacecraftstatus.write_to_sink(df=df)
    return df

@asset
def config_spacestationstatus_prod() -> DataFrame:
    config_spacestationstatus = BaseAPI(library="config/spacestationstatus", env = "prod")
    df = config_spacestationstatus.get_api_data()
    config_spacestationstatus.write_to_sink(df=df)
    return df

@asset
def docking_event_prod(config_dockinglocation_prod) -> DataFrame:
    docking_event = BaseAPI(library="docking_event", filter="docking__gt", env = "prod")
    df = docking_event.get_api_data()
    docking_event.write_to_sink(df=df)
    return df

@asset
def event_prod(config_eventtype_prod) -> DataFrame:
    event = BaseAPI(library="event", env = "prod")
    df = event.get_api_data()
    df = parse_json_col(df=df, column="launches", new_col_name="launch_id")
    df = parse_json_col(df=df, column="updates", new_col_name="update_id")
    df = parse_json_col(df=df, column="expeditions", new_col_name="expedition_id")
    df = parse_json_col(df=df, column="spacestations", new_col_name="spacestation_id")
    df = parse_json_col(df=df, column="program", new_col_name="program_id")
    event.write_to_sink(df=df)
    return df

@asset
def expedition_prod(config_missiontype_prod, config_orbit_prod) -> DataFrame:
    expedition = BaseAPI(library="expedition", env = "prod")
    df = expedition.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="mission_patches", new_col_name="mission_patch_id", drop_col=False)
    df = parse_json_col(df=df, index_label="name", column="mission_patches", new_col_name="mission_patch_name", drop_col=False)
    df = parse_json_col(df=df, index_label="priority", column="mission_patches", new_col_name="mission_priority", drop_col=False)
    df = parse_json_col(df=df, index_label="image_url", column="mission_patches", new_col_name="mission_patch_image_url")
    expedition.write_to_sink(df=df)
    return df

@asset
def launch_prod(config_firststagetype_prod) -> DataFrame:
    launch = BaseAPI(library="launch", env = "prod")
    df = launch.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="program", new_col_name="program_id")
    launch.write_to_sink(df=df)
    return df

@asset
def launcher_prod(config_launcher_prod, config_launchstatus_prod) -> DataFrame:
    launcher = BaseAPI(library="launcher", env = "prod")
    df = launcher.get_api_data()
    launcher.write_to_sink(df=df)
    return df

@asset
def location_prod(config_landinglocation_prod) -> DataFrame:
    location = BaseAPI(library="location", env = "prod")
    df = location.get_api_data()
    location.write_to_sink(df=df)
    return df

@asset
def pad_prod() -> DataFrame:
    pad = BaseAPI(library="pad", env = "prod")
    df = pad.get_api_data()
    pad.write_to_sink(df=df)
    return df

@asset
def program_prod() -> DataFrame:
    program = BaseAPI(library="program", env = "prod")
    df = program.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="agencies", new_col_name="agency_id")
    program.write_to_sink(df=df)
    return df

@asset
def spacecraft_prod(config_spacecraft_prod, config_spacecraftstatus_prod) -> DataFrame:
    spacecraft = BaseAPI(library="spacecraft", env = "prod")
    df = spacecraft.get_api_data()
    spacecraft.write_to_sink(df=df)
    return df

@asset
def spacecraft_flight_prod(config_spacecraft_prod, config_spacecraftstatus_prod, expedition_prod, event_prod) -> DataFrame:
    spacecraft_flight = BaseAPI(library="spacecraft/flight", env = "prod")
    df = spacecraft_flight.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="launch_program", new_col_name="launch_program_id")
    spacecraft_flight.write_to_sink(df=df)
    return df

@asset
def spacestation_prod(config_spacestationstatus_prod) -> DataFrame:
    spacestation = BaseAPI(library="spacestation", env = "prod")
    df = spacestation.get_api_data()
    df = parse_json_col(df=df, index_label="id", column="owners", new_col_name="owner_id")
    spacestation.write_to_sink(df=df)
    return df

@asset
def updates_prod() -> DataFrame:
    updates = BaseAPI(library="updates", env = "prod")
    df = updates.get_api_data()
    updates.write_to_sink(df=df)
    return df

pg_user = os.environ.get("pg_user")
pg_password = os.environ.get("pg_password")
pg_host = os.environ.get("pg_host")
pg_port = os.environ.get("pg_port")
pg_db = os.environ.get("pg_db")
write_engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")