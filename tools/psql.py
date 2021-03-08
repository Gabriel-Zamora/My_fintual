from psycopg2 import connect
import pandas as pd
import os

def psql(auth):
    con = connect(dbname = auth['dbname'], user = auth['user'], host = auth['host'], password = os.environ[auth['password']], port = auth['port'])
    return con

def psql_start(auth, BBDD):

    try:
        con = psql(auth)
        cur = con.cursor()
        cur.execute(f"DROP SCHEMA {BBDD['schema']} CASCADE")
        con.commit()
        con.close()
    except:
        pass

    con = psql(auth)
    cur = con.cursor()

    query = f"CREATE SCHEMA {BBDD['schema']};"

    cur.execute(query)
    con.commit()
    con.close()

    for table in list(BBDD['tables'].keys()):
        psql_create_table(auth, BBDD,table)    

def psql_create_table(auth, BBDD, table):

    try:
        con = psql(auth)
        cur = con.cursor()
        cur.execute(f"DROP TABLE {BBDD['schema']}.{BBDD['tables'][table]['name']} CASCADE")
        con.commit()
        con.close()
    except:
        pass

    con = psql(auth)
    cur = con.cursor()

    query = f"\nCREATE TABLE {BBDD['schema']}.{BBDD['tables'][table]['name']} ("
    
    for j in range(len(list(BBDD['tables'][table]['columns']))):
        column = BBDD['tables'][table]['columns'][list(BBDD['tables'][table]['columns'].keys())[j]]

        unique = ''
        if list(BBDD['tables'][table]['columns'].keys())[j] in BBDD['tables'][table]['unique']:
            unique = f" NOT NULL" 

        default = ''
        if 'default' in list(column.keys()):
            if column['type'].lower() in ['int', 'integer', 'smallint', 'bigint', 'numeric', 'decimal','serial', 'real', 'bigserial','boolean','bool']:
                default = f" default {column['default']}"

            elif column['default'] == 'NULL':
                default = f" default {column['default']}"

            else:
                default = f" default '{column['default']}'"

        query += f"{column['name']} {column['type']}{unique}{default}, "

    query += f"CONSTRAINT {BBDD['tables'][table]['name']}_pkey PRIMARY KEY ({BBDD['tables'][table]['columns'][BBDD['tables'][table]['key']]['name']})"

    for column in list(BBDD['tables'][table]['columns'].keys()):
        if column in list(BBDD['tables'][table]['foreign'].keys()):
            col = BBDD['tables'][table]['foreign'][column]
            query += f", CONSTRAINT foreign_{BBDD['tables'][table]['columns'][column]['name']} "
            query += f"FOREIGN KEY ({BBDD['tables'][table]['columns'][column]['name']}) "
            query += f"REFERENCES {BBDD['schema']}.{BBDD['tables'][col['table']]['name']} ({BBDD['tables'][col['table']]['columns'][col['column']]['name']}) MATCH SIMPLE "
            query += f"ON UPDATE {col['update']} "
            query += f"ON DELETE {col['delete']}"

    query += ");"

    cur.execute(query)
    con.commit()
    con.close()    

def psql_insert(auth, BBDD, bd, json):
    
    if len(list(json.keys())) == 0:
        return None

    schema = BBDD['schema']
    base = BBDD['tables'][bd]['name']
    key = BBDD['tables'][bd]['columns'][BBDD['tables'][bd]['key']]['name']

    if BBDD['tables'][bd]['key'] in list(json.keys()):
        for col in list(BBDD['tables'][bd]['columns']):
            if col.lower() not in [col for col in json.keys()]:
                json[col.lower()] = 'NULL'

    headers = [] 
    for col in list(BBDD['tables'][bd]['columns']):
        if col.lower() in [col for col in json.keys()]:
            headers.append(col.lower())

    select = ""
    columns = "("
    values = "("
    for i in range(len(headers)-1):

        if BBDD['tables'][bd]['columns'][headers[i]]['type'].lower() in ['int', 'integer', 'smallint', 'bigint', 'numeric', 'decimal','serial', 'real', 'bigserial','boolean','bool']:
            select += f" {json[headers[i]]} AS {BBDD['tables'][bd]['columns'][headers[i]]['name']}, "
            columns += f" {BBDD['tables'][bd]['columns'][headers[i]]['name']}, "
            values += f" {json[headers[i]]}, "

        elif json[headers[i]] == 'NULL':
            select += f" {json[headers[i]]} AS {BBDD['tables'][bd]['columns'][headers[i]]['name']}, "
            columns += f" {BBDD['tables'][bd]['columns'][headers[i]]['name']}, "
            values += f" {json[headers[i]]}, "

        else:
            select += f" '{json[headers[i]]}' AS {BBDD['tables'][bd]['columns'][headers[i]]['name']}, "
            columns += f" {BBDD['tables'][bd]['columns'][headers[i]]['name']}, "
            values += f" '{json[headers[i]]}', "

    if BBDD['tables'][bd]['columns'][headers[len(headers)-1]]['type'].lower() in ['int', 'integer', 'smallint', 'bigint', 'numeric', 'decimal','serial', 'real', 'bigserial','boolean','bool']:
        select += f" {json[headers[len(headers)-1]]} AS {BBDD['tables'][bd]['columns'][headers[len(headers)-1]]['name']}"
        columns += f" {BBDD['tables'][bd]['columns'][headers[len(headers)-1]]['name']} )"
        values += f" {json[headers[len(headers)-1]]} )"

    elif json[headers[len(headers)-1]] == 'NULL':
        select += f" {json[headers[len(headers)-1]]} AS {BBDD['tables'][bd]['columns'][headers[len(headers)-1]]['name']}"
        columns += f" {BBDD['tables'][bd]['columns'][headers[len(headers)-1]]['name']} )"
        values += f" {json[headers[len(headers)-1]]} )"

    else:
        select += f" '{json[headers[len(headers)-1]]}' AS {BBDD['tables'][bd]['columns'][headers[len(headers)-1]]['name']}"
        columns += f" {BBDD['tables'][bd]['columns'][headers[len(headers)-1]]['name']} )"
        values += f" '{json[headers[len(headers)-1]]}' )"


    if BBDD['tables'][bd]['key'] in list(json.keys()):

        if BBDD['tables'][bd]['columns'][BBDD['tables'][bd]['key']]['type'] in ['int', 'integer', 'smallint', 'bigint', 'numeric', 'decimal','serial', 'real', 'bigserial','boolean','bool']:
            key_val = json[key]
        
        else:
            key_val = f"'{json[key]}'"

        query = f"""
        UPDATE {schema}.{base}
        SET {columns} = {values}
        WHERE {key} = {key_val};\n

        INSERT INTO {schema}.{base}
        SELECT {select}
        WHERE {key_val} NOT IN (
            SELECT {key}
            FROM {schema}.{base}
        );
        """

    else:
        query = f"""
        INSERT INTO {schema}.{base} {columns} VALUES {values}
        """

    con = psql(auth)

    cur = con.cursor()
    cur.execute(query)
    con.commit()

    if BBDD['tables'][bd]['columns'][BBDD['tables'][bd]['key']]['type'].lower() in ['serial','bigserial']:
        query = f"""
        SELECT MAX({BBDD['tables'][bd]['columns'][BBDD['tables'][bd]['key']]['name']})
        FROM {schema}.{base}
        """

        cur.execute(query)
        maxi = cur.fetchall()[0][0]+1

        query = f"ALTER SEQUENCE {schema}.{base}_{key}_seq RESTART WITH {maxi}"
        cur.execute(query)
        con.commit()

    con.close()   

def psql_insert_many(auth, BBDD, df, bd):

    if len(df) == 0:
        return None

    schema = BBDD['schema']
    base = BBDD['tables'][bd]['name']
    key = BBDD['tables'][bd]['columns'][BBDD['tables'][bd]['key']]['name']

    table = []
    for i in list(df.index):
        row = dict()

        for col in list(df.columns):
            if df[df.index == i][col].isnull().values[0]:
                row[col] = None
            else:
                row[col] = df[df.index == i][col].values[0]

        table.append(row)


    select = ""
    columns = "("
    values = "("
    for i in range(len(df.columns)-1):
        select += f" %({df.columns[i]})s AS {df.columns[i]}, "
        columns += f" {df.columns[i]}, "
        values += f" %({df.columns[i]})s, "

    select += f" %({df.columns[len(df.columns)-1]})s AS {df.columns[len(df.columns)-1]}"
    columns += f" {df.columns[len(df.columns)-1]} )"
    values += f" %({df.columns[len(df.columns)-1]})s )"


    query1 = f"""
    INSERT INTO {schema}.{base}
    SELECT {select}
    WHERE %({key})s NOT IN (
        SELECT {key}
        FROM {schema}.{base}
    )
    """

    query2 = f"""
    UPDATE {schema}.{base}
    SET {columns} = {values}
    WHERE {key} = %({key})s
    """

    con = psql(auth)

    cur = con.cursor()
    cur.executemany(query2, table)
    cur.executemany(query1, table)
    con.commit()

    if BBDD['tables'][bd]['columns'][BBDD['tables'][bd]['key']]['type'].lower() in ['serial','bigserial']:
        query = f"""
        SELECT MAX({BBDD['tables'][bd]['columns'][BBDD['tables'][bd]['key']]['name']})
        FROM {schema}.{base}
        """

        cur.execute(query)
        maxi = cur.fetchall()[0][0]+1

        query = f"ALTER SEQUENCE {schema}.{base}_{key}_seq RESTART WITH {maxi}"
        cur.execute(query)
        con.commit()

    con.close()
    
jquery = {'headers':'*','constraints':'','join':'','grouped':''}
def psql_query(auth, BBDD, bd, jquery = {'headers':'*','constraints':'','join':'','grouped':''}):

    table = BBDD['tables'][bd]['name']
    schema = BBDD['schema']

    query = f"""
    SELECT {jquery['headers']}
    FROM {schema}.{table}
    {jquery['join']}
    {jquery['constraints']}
    {jquery['grouped']}
    """

    con = psql(auth)
    cur = con.cursor()

    cur.execute(query)
    columns = [col[0] for col in cur.description]
    data = cur.fetchall() 

    con.close()

    df = pd.DataFrame(data, columns = columns, dtype = str)
    return df