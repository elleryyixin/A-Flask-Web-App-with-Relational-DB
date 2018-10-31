import pymysql
import json

cnx = pymysql.connect(host='localhost',
                              user='root',
                              password='dbuser',
                              db='lahman2017_raw',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)


def run_q(q, args, fetch=False):
    cursor = cnx.cursor()
    cursor.execute(q, args)
    if fetch:
        result = cursor.fetchall()
    else:
        result = None
    cnx.commit()
    return result


def find_query_generation(table, t, fields, limit, offset):

    s = ""
    for k, v in t.items():
        if isinstance(v, list):
            v = v[0]
        if s != "":
            s += " AND "
        s += k + "='" + v + "'"

    if s != "":
        s = "WHERE " + s

    if not fields:
        s = "SELECT * FROM " + table + ' ' + s

    else:
        r = ", ".join(fields)
        s = "SELECT " + r + " FROM " + table + ' ' + s + " LIMIT " + limit + \
            " OFFSET " + offset

    return s


def insert_query_generation(table, r):
    c = ""
    after_values = ""
    for k, v in r.items():
        if c != "":
            c += ", "
        if after_values != "":
            after_values += ", "

        after_values += "%s"
        c += k

    c = " (" + c + ") "
    after_values = " (" + after_values + ") "

    q = "INSERT INTO " + table + c + "VALUES" + after_values

    return q


def update_query_generation(table, t, update):

    after_set = ""
    for k, v in update.items():
        if after_set != "":
            after_set += ", "
        after_set += k + "='" + v + "'"

    after_where = ""
    for k, v in t.items():
        if after_where != "":
            after_where += " AND "
        after_where += k + "='" + v + "'"

    q = "UPDATE " + table + " SET " + after_set + " WHERE " + after_where

    return q


def delete_query_generation(table, t):

    s = ""
    for k, v in t.items():
        if s != "":
            s += " AND "
        s += k + "='" + v + "'"

    if s != "":
        s = "WHERE " + s

    s = "DELETE" + " FROM " + table + ' ' + s

    return s


def str2sqlstr(str):
    if str:
        return "'" + str + "'"


def extract_primary_key(table):

    # sql query for extracting primary keys from table
    q = 'SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = ' + \
        str2sqlstr('lahman2017_raw') + ' and TABLE_NAME = ' + str2sqlstr(table) + \
        ' AND CONSTRAINT_NAME = ' + "'PRIMARY'"
    print q
    raw_results = run_q(q, None, True)
    result = []
    for raw in raw_results:
        result.append(raw.get('COLUMN_NAME'))
    return result


def find_by_template(table, template, fields, limit, offset):

    q = find_query_generation(table, template, fields, limit, offset)

    result = run_q(q, None, True)
    return result


def insert_row(table, row):

    q = insert_query_generation(table, row)

    print q

    run_q(q, row.values(), False)


def find_by_primary_key(table, s, fields, limit, offset):
    '''
    Return a table containing the row matching the primary key and field selector.
    :param table: Table name.
    :param s: List of strings of primary key values that the rows much match.
    :param fields: A list of columns to include in responses.
    :return: Table containing the answer.
    '''
    primary_keys = extract_primary_key(table)

    s = s.split('_')

    t = dict(zip(primary_keys, s))

    q = find_query_generation(table, t, fields, limit, offset)
    print "q" + q
    result = run_q(q, None, True)

    return result


def update_row(table, s, update):

    primary_keys = extract_primary_key(table)

    s = s.split('_')

    t = dict(zip(primary_keys, s))

    q = update_query_generation(table, t, update)

    print q
    run_q(q, None, False)


def delete_row(table, s):

    primary_keys = extract_primary_key(table)

    s = s.split('_')

    t = dict(zip(primary_keys, s))

    q = delete_query_generation(table, t)

    print q
    run_q(q, None, False)


def find_related_rows(table, s, related_table, query, fields, limit, offset):

    primary_keys = extract_primary_key(table)
    s = s.split('_')

    t = dict(zip(primary_keys, s))

    related_primary_keys = extract_primary_key(related_table)

    related_t = {}
    for key in related_primary_keys:
        if key in primary_keys:
            related_t[key] = t[key]

    print "related_t"
    print related_t

    if not related_t:
        raise ValueError("No Relation Between Two Tables")

    related_t.update(query)

    print related_t
    q = find_query_generation(related_table, related_t, fields, limit, offset)
    print "did not stop here"
    print q
    result = run_q(q, None, True)

    return result


def extract_all_columns(table):
    q = "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema='lahman2017_raw' AND table_name=" + \
        str2sqlstr(table)
    raw_results = run_q(q, None, True)
    result = []
    for raw in raw_results:
        result.append(raw.get('COLUMN_NAME'))
    return result


def update_related_row(table, s, related_table, body):

    try:
        result = find_by_primary_key(table, s, None, 1, 0)
        if not result:
            raise ValueError('Nonexistent primary key value')
    except Exception as e:
        print("Got exception: ", e)

    primary_keys = extract_primary_key(table)
    related_keys = extract_all_columns(related_table)

    for key in primary_keys:
        if key not in related_keys:
            raise ValueError("No primary key pair in related table")
    if body:
        s = s.split('_')
        t = dict(zip(primary_keys, s))

        related_t = {}
        for key in related_keys:
            if key in primary_keys:
                related_t[key] = t[key]

        print related_t

        related_t.update(body)

        values = related_t.values()
        q = insert_query_generation(related_table, related_t)

        run_q(q, values, False)


def find_teammates(playerid, limit, offset):

    try:
        q1 = "CREATE VIEW playerID_teamID_yearID \
        as SELECT \
        People.playerID, Batting.teamID, Batting.yearID \
        from Batting join People \
        on People.playerID = Batting.playerID;"

        run_q(q1, None, False)
    except Exception as e:
        print("Warning = ", e)

    try:
        q2 ="CREATE VIEW \
            teammates_playerid_teamid_yearid as \
            SELECT \
            playerID_teamID_yearID.playerID as playerID, \
            Batting.playerID as teammateID, \
            Batting.teamID as teamID, \
            Batting.yearID as yearID \
            from playerID_teamID_yearID join Batting \
            on playerID_teamID_yearID.teamID = Batting.teamID \
            and playerID_teamID_yearID.yearID = Batting.yearID \
            and playerID_teamID_yearID.playerID != Batting.playerID;"

        run_q(q2, None, False)
    except Exception as e:
        print("Warning = ", e)

    try:
        q3 = "CREATE VIEW teammates_info as\
            SELECT \
            teammates_playerid_teamid_yearid.playerID as playerID,\
            teammates_playerid_teamid_yearid.yearID as yearID,\
            teammates_playerid_teamid_yearid.teammateID as teammateID,\
            teammates_playerid_teamid_yearid.teamID as teamID,\
            People.nameFirst as first_name,\
            People.nameLast as last_name\
            from teammates_playerid_teamid_yearid join People \
            on teammates_playerid_teamid_yearid.teammateID = People.playerID;"

        run_q(q3, None, False)
    except Exception as e:
        print("Warning = ", e)

    q4 = "SELECT \
        playerID as player_ID, \
        teammateID as teammate_ID, \
        first_name, \
        last_name,\
        min(yearID) as first_year,\
        max(yearID) as last_year,\
        count(yearID) as count_of_seasons\
        FROM teammates_info \
        WHERE playerID = " + str2sqlstr(playerid) + \
        " GROUP BY playerID, teammateID " + "LIMIT " + limit + " OFFSET " + offset + ";"

    print q4
    result = run_q(q4, None, True)

    return result


def find_career_stats(playerid, limit, offset):

    try:
        q1 = " CREATE VIEW A_E_AB_H as\
        SELECT \
        Batting.playerID, \
        Batting.yearID, \
        Batting.teamID, \
        sum(Fielding.A) as A, \
        sum(Fielding.E) as E, \
        sum(Batting.AB) as AB, \
        sum(Batting.H) as H \
        from Batting join Fielding on \
        Batting.playerID = Fielding.playerID \
        and Batting.yearID = Fielding.yearID \
        and Batting.teamID = Fielding.teamID \
        GROUP BY \
        Batting.playerID, Batting.yearID, Batting.teamID;"

        run_q(q1, None, False)
    except Exception as e:
        print("Warning = ", e)

    q2 = "SELECT \
        A_E_AB_H.playerID,\
        A_E_AB_H.yearID,\
        A_E_AB_H.teamID,\
        Appearances.G_all,\
        A_E_AB_H.A, \
        A_E_AB_H.E, \
        A_E_AB_H.AB, \
        A_E_AB_H.H \
        from Appearances join A_E_AB_H on \
        Appearances.playerID = A_E_AB_H.playerID and \
        Appearances.teamID = A_E_AB_H.teamID and \
        Appearances.yearID = A_E_AB_H.yearID \
        WHERE Appearances.playerID = " + str2sqlstr(playerid) + " LIMIT " + limit + " OFFSET " \
         + offset + ";"

    print q2

    result = run_q(q2, None, True)

    return result


def find_roster_stats(teamid, yearid, limit, offset):

    try:
        q1 = "CREATE VIEW partial_stats as \
        SELECT \
        Batting.playerID, \
        Batting.yearID, \
        Batting.teamID, \
        sum(Fielding.A) as A, \
        sum(Fielding.E) as E, \
        Batting.AB as AB, \
        Batting.H as H \
        from Fielding join \
        Batting \
        on \
        Fielding.playerID = Batting.playerID \
        and Fielding.yearID = Batting.yearID \
        and Fielding.teamID = Batting.teamID \
        and Fielding.stint = Batting.stint \
        GROUP BY \
        Batting.playerID, Batting.yearID, Batting.teamID, Batting.stint;"

        print "running q1"
        run_q(q1, None, False)
    except Exception as e:
        print("Warning = ", e)

    try:

        q2 = "CREATE VIEW career_stats as \
        SELECT \
        partial_stats.playerID, \
        partial_stats.yearID, \
        partial_stats.teamID, \
        Appearances.G_all, \
        partial_stats.A, \
        partial_stats.E, \
        partial_stats.AB, \
        partial_stats.H \
        from Appearances join partial_stats \
        on Appearances.playerID = partial_stats.playerID and  \
        Appearances.teamID = partial_stats.teamID and  \
        Appearances.yearID = partial_stats.yearID \
        WHERE Appearances.teamID =" + str2sqlstr(teamid) + " and Appearances.yearID = " \
             + str2sqlstr(yearid)

        print "running q2"
        run_q(q2, None, False)

    except Exception as e:
        print("Warning = ", e)

    q3 = "SELECT \
        (select People.nameFirst from People where People.playerID = career_stats.playerID) as first_name,\
        (select People.nameLast from People where People.playerID = career_stats.playerID) as last_name,\
        playerID,\
        teamID,\
        yearID,\
        G_all,\
        H,\
        AB,\
        A,\
        E\
        FROM career_stats" + " LIMIT " + limit + " OFFSET " + offset + ";"

    print q3

    result = run_q(q3, None, True)

    return result




