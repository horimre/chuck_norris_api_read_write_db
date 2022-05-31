import datetime
import json
import logging

import pandas as pd
import requests
import sqlalchemy as db
from requests.exceptions import HTTPError

import DbConf

# set log level
logging.basicConfig(level=logging.WARNING)

# set sql engine
sql_engine = db.create_engine(f'mysql+pymysql://{DbConf.db_conf["user"]}:'
                              f'{DbConf.db_conf["password"]}'
                              f'@localhost:{DbConf.db_conf["port"]}/'
                              f'{DbConf.db_conf["db"]}')

url = "https://api.chucknorris.io/jokes/random"


def load_to_db(df: pd.Series, engine: db.engine.base.Engine):
    try:

        meta = db.MetaData()

        chuck = db.Table(
            DbConf.db_conf["table_name"],
            meta,
            db.Column('created', db.DateTime),
            db.Column('icon_url', db.String(4000)),
            db.Column('id', db.String(4000)),
            db.Column('url', db.String(4000)),
            db.Column('value', db.String(4000))
        )

        meta.create_all(engine)

        stmt = db.insert(chuck).values(created=datetime.datetime.now(),
                                       icon_url=df.icon_url,
                                       id=df.id,
                                       url=df.url,
                                       value=df.value)

        with engine.connect() as conn:
            conn.execute(stmt)

    except Exception:
        # logging.error(f"Error: {e}", exc_info=True)
        raise


def read_from_db(engine: db.engine.base.Engine):
    try:
        df = pd.read_sql(
            f'SELECT * FROM {DbConf.db_conf["table_name"]}', engine)
        df.to_csv('out_db_result.csv', sep=',', index=False)
    except Exception:
        # logging.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":

    try:
        response = requests.get(url)

        response.raise_for_status()

        response_json = response.json()

        # dump response_json to {topic}_{company}.json file
        with open('chuck.json', 'w') as file:
            json.dump(response_json, file, indent=4)

        fact = pd.Series(response_json)

        print(fact.value)

        load_to_db(fact, sql_engine)
        read_from_db(sql_engine)

    except HTTPError as http_err:
        logging.error(
            f"Request failed. HTTP error: {http_err} Message: {response.text}")
        raise
    except Exception as e:
        logging.error(f"Other Error: {e}", exc_info=True)
    else:
        logging.info(f"Request finished successfully. "
                     f"Status code: {response.status_code} "
                     f"Reason: {response.reason}")
