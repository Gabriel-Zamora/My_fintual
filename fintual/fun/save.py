import os
import sys; sys.path.append(f"{os.environ['ROOT_PATH_FINTUAL']}my_fintual/fintual/fun")
import sys; sys.path.append(f"{os.environ['ROOT_PATH_FINTUAL']}my_fintual/tools")
from get import *
from psql import *
import json

with open(f"{os.environ['ROOT_PATH_FINTUAL']}my_fintual/config/auth.json") as json_file: 
    auth = json.load(json_file)
with open(f"{os.environ['ROOT_PATH_FINTUAL']}my_fintual/config/BBDD.json") as json_file: 
    BBDD  = json.load(json_file)['fintual']

def save_assets():
    df = get_assets()
    psql_insert_many(auth, BBDD, df, 'assets')

def save_funds():

    assets = [2643,38,37,36]

    for id_asset in assets:
        df = get_fund(id_asset)
        psql_insert_many(auth, BBDD, df, 'funds')

def save_series():

    funds = psql_query(auth, BBDD, 'funds')

    for id_fund in list(funds[BBDD['tables']['funds']['columns'][BBDD['tables']['funds']['key']]['name']]):

        query = jquery.copy()
        query['headers'] = f"max({BBDD['tables']['series']['columns']['date']['name']})"
        query['constraints'] = f"where {BBDD['tables']['series']['columns']['id_fund']['name']} = {id_fund}"
        start_date = psql_query(auth, BBDD, 'series',query).values[0][0]

        if start_date == None:
            df = get_serie(funds, id_fund)
        else:
            df = get_serie(funds, id_fund, start_date)

        psql_insert_many(auth, BBDD, df, 'series')

def save_my_goals():
    df = get_my_goals()
    psql_insert_many(auth, BBDD, df, 'my_goals')

def save_my_serie(id_goal):
    json = get_my_serie(id_goal)
    psql_insert(auth, BBDD, 'my_series', json)

def save_my_investment(id_goal):
    df = get_my_investment(id_goal)

    for id_fund in list(df['id_fund']):
        
        query = jquery.copy()
        query['headers'] = f"{BBDD['tables']['series']['columns']['date']['name'] }, {BBDD['tables']['series']['columns']['price']['name'] }"
        query['constraints'] = f"""
            WHERE {BBDD['tables']['series']['columns']['id_fund']['name']} = {id_fund} 
            AND date <= '{df[df['id_fund'] == id_fund]['date'].values[0]}'"""
        query['grouped'] = f"ORDER BY {BBDD['tables']['series']['columns']['date']['name']} DESC LIMIT 1"

        fund = psql_query(auth, BBDD, 'series', query)
        
        date = fund[BBDD['tables']['series']['columns']['date']['name']].values[0]
        df.loc[df['id_fund'] == id_fund, BBDD['tables']['my_investments']['columns']['id_serie']['name']] = f"{id_fund}-{date}"

        nav = float(df[df['id_fund'] == id_fund][BBDD['tables']['my_investments']['columns']['nav']['name']].values[0])
        price = float(fund[BBDD['tables']['series']['columns']['price']['name']].values[0])
        shares = round(nav/price,4)
        df.loc[df['id_fund'] == id_fund, BBDD['tables']['my_investments']['columns']['shares']['name']] = shares

    del df['id_fund']

    psql_insert_many(auth, BBDD, df, 'my_investments')

def save_my_fintual():
    goals = list(psql_query(auth, BBDD, 'my_goals')[BBDD['tables']['my_goals']['columns']['id']['name']])

    for goal in goals:
        save_my_serie(goal)
        save_my_investment(goal)