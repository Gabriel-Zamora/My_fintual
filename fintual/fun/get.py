import os
import sys; sys.path.append(f"{os.environ['ROOT_PATH_FINTUAL']}my_fintual/tools")
import requests
import json
import pandas as pd
import datetime as dt 

with open(f"{os.environ['ROOT_PATH_FINTUAL']}my_fintual/config/BBDD.json") as json_file: 
    BBDD  = json.load(json_file)['fintual']
with open(f"{os.environ['ROOT_PATH_FINTUAL']}my_fintual/fintual/config/fintual_api.json") as json_file: 
    api  = json.load(json_file)

def get_assets():

    df = pd.DataFrame(columns = [BBDD['tables']['assets']['columns']['id']['name'] ,
                                 BBDD['tables']['assets']['columns']['name']['name'] ,
                                 BBDD['tables']['assets']['columns']['symbol']['name'] ,
                                 BBDD['tables']['assets']['columns']['category']['name'] ,
                                 BBDD['tables']['assets']['columns']['data_source']['name'] ])

    assets = requests.get( api['url'] + api['assets'] ).json()['data']

    for i in range(len(assets)):
        df = df.append(
            {BBDD['tables']['assets']['columns']['id']['name']          : assets[i]['id'], 
             BBDD['tables']['assets']['columns']['name']['name']        : assets[i]['attributes']['name'],
             BBDD['tables']['assets']['columns']['symbol']['name']      : assets[i]['attributes']['symbol'],
             BBDD['tables']['assets']['columns']['category']['name']    : assets[i]['attributes']['category'],
             BBDD['tables']['assets']['columns']['data_source']['name'] : assets[i]['attributes']['data_source']
            } ,ignore_index=True)

    return df

def get_fund(id_asset):
    df = pd.DataFrame(columns = [BBDD['tables']['funds']['columns']['id']['name'],
                                 BBDD['tables']['funds']['columns']['id_asset']['name'],
                                 BBDD['tables']['funds']['columns']['name']['name'],
                                 BBDD['tables']['funds']['columns']['symbol']['name'],
                                 BBDD['tables']['funds']['columns']['serie']['name'],
                                 BBDD['tables']['funds']['columns']['start_date']['name'],
                                 BBDD['tables']['funds']['columns']['last_date']['name']])

    json_assets = requests.get( api['url'] + api['assets'] + '/' + str(id_asset) + '/' + api['real'] ).json()['data']

    for i in range(len(json_assets)):
        df = df.append(
            {
            BBDD['tables']['funds']['columns']['id']['name']            : json_assets[i]['id'], 
            BBDD['tables']['funds']['columns']['id_asset']['name']      : id_asset,
            BBDD['tables']['funds']['columns']['name']['name']          : json_assets[i]['attributes']['name'],
            BBDD['tables']['funds']['columns']['symbol']['name']        : json_assets[i]['attributes']['symbol'],
            BBDD['tables']['funds']['columns']['serie']['name']         : json_assets[i]['attributes']['serie'],
            BBDD['tables']['funds']['columns']['start_date']['name']    : json_assets[i]['attributes']['start_date'],  
            BBDD['tables']['funds']['columns']['last_date']['name']     : json_assets[i]['attributes']['last_day']['date']
            } ,ignore_index=True)

    return df

def get_serie(funds, id_fund, date_start = '1980-02-10'):

    df = pd.DataFrame(columns = [BBDD['tables']['series']['columns']['id']['name'],
                                BBDD['tables']['series']['columns']['id_fund']['name'],
                                BBDD['tables']['series']['columns']['name']['name'],
                                BBDD['tables']['series']['columns']['serie']['name'],
                                BBDD['tables']['series']['columns']['date']['name'],
                                BBDD['tables']['series']['columns']['price']['name']])

    now = dt.datetime.now()

    funds    = funds[funds[BBDD['tables']['funds']['columns']['id']['name']] == id_fund]
    name     = funds[BBDD['tables']['funds']['columns']['name']['name']].values[0]
    serie    = funds[BBDD['tables']['funds']['columns']['serie']['name']].values[0]
    date_end = f"{now.year}-{now.month}-{now.day}"

    url = api['url'] + api['real'] + '/' + id_fund + '/days?from_date=' + date_start + '&to_date=' + date_end
    json_serie = requests.get(url).json()['data']

    for i in range(len(json_serie)):
            df = df.append(
                    {      
                    BBDD['tables']['series']['columns']['id']['name']      : json_serie[i]['id'], 
                    BBDD['tables']['series']['columns']['id_fund']['name'] : id_fund,
                    BBDD['tables']['series']['columns']['name']['name']    : name,
                    BBDD['tables']['series']['columns']['serie']['name']   : serie,
                    BBDD['tables']['series']['columns']['date']['name']    : json_serie[i]['attributes']['date'] ,
                    BBDD['tables']['series']['columns']['price']['name']   : json_serie[i]['attributes']['price'] 
                    } ,ignore_index=True)

    return df

def get_my_goals():

    df = pd.DataFrame(columns = [BBDD['tables']['my_goals']['columns']['id']['name'] ,
                                BBDD['tables']['my_goals']['columns']['name']['name'] ,
                                BBDD['tables']['my_goals']['columns']['created']['name'] ])

    url = f"{api['url']}{api['goals']}/?user_token={os.environ[api['user_token']]}&user_email={os.environ[api['user_email']]}"
    my_goals = requests.get( url ).json()['data']

    for i in range(len(my_goals)):
        df = df.append(
            {
                BBDD['tables']['my_goals']['columns']['id']['name']      : my_goals[i]['id'], 
                BBDD['tables']['my_goals']['columns']['name']['name']    : my_goals[i]['attributes']['name'],
                BBDD['tables']['my_goals']['columns']['created']['name'] : my_goals[i]['attributes']['created_at'][0:10] 
            } ,ignore_index=True)

    return df

def get_my_serie(id_goal):
    json = {}

    url = f"{api['url']}{api['goals']}/{id_goal}?user_token={os.environ[api['user_token']]}&user_email={os.environ[api['user_email']]}"
    my_serie = requests.get( url ).json()['data']

    if my_serie == 'ActiveRecord::RecordNotFound':
        return json

    now = dt.datetime.now()
    date = f"{str(now.year).zfill(4)}-{str(now.month).zfill(2)}-{str(now.day).zfill(2)}"

    json[BBDD['tables']['my_series']['columns']['id']['name']]        = f"{my_serie['id']}-{date}"
    json[BBDD['tables']['my_series']['columns']['id_goal']['name']]   = my_serie['id']
    json[BBDD['tables']['my_series']['columns']['name']['name']]      = my_serie['attributes']['name']
    json[BBDD['tables']['my_series']['columns']['date']['name']]      = date
    json[BBDD['tables']['my_series']['columns']['deposited']['name']] = my_serie['attributes']['deposited'] 
    json[BBDD['tables']['my_series']['columns']['profit']['name']]    = my_serie['attributes']['profit'] 
    json[BBDD['tables']['my_series']['columns']['nav']['name']]       = my_serie['attributes']['nav'] 

    return json

def get_my_investment(id_goal):
    df = pd.DataFrame(columns = [BBDD['tables']['my_investments']['columns']['id']['name'],
                                BBDD['tables']['my_investments']['columns']['id_serie']['name'],
                                BBDD['tables']['my_investments']['columns']['id_my_serie']['name'],
                                BBDD['tables']['my_investments']['columns']['date']['name'],
                                BBDD['tables']['my_investments']['columns']['weight']['name'],
                                'id_fund'])

    url = f"{api['url']}{api['goals']}/{id_goal}?user_token={os.environ[api['user_token']]}&user_email={os.environ[api['user_email']]}"
    my_investment = requests.get( url ).json()['data']

    if my_investment == 'ActiveRecord::RecordNotFound':
        return df

    now = dt.datetime.now()
    date = f"{str(now.year).zfill(4)}-{str(now.month).zfill(2)}-{str(now.day).zfill(2)}"

    for i in range(len(my_investment['attributes']['investments'])):
        nav = round(my_investment['attributes']['investments'][i]['weight']*my_investment['attributes']['nav'],4)

        df = df.append(
            {
                BBDD['tables']['my_investments']['columns']['id']['name']           : f"{my_investment['id']}-{my_investment['attributes']['investments'][i]['asset_id']}-{date}",
                BBDD['tables']['my_investments']['columns']['id_serie']['name']     : None,
                BBDD['tables']['my_investments']['columns']['id_my_serie']['name']  : f"{my_investment['id']}-{date}",
                BBDD['tables']['my_investments']['columns']['date']['name']         : date,
                BBDD['tables']['my_investments']['columns']['weight']['name']       : my_investment['attributes']['investments'][i]['weight'],
                'id_fund'                                                           : f"{my_investment['attributes']['investments'][i]['asset_id']}",
            } ,ignore_index=True)
    
    return df
