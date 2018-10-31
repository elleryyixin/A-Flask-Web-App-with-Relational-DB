# Lahman.py

# Convert to/from web native JSON and Python/RDB types.
import json

# Include Flask packages
from flask import Flask
from flask import request
import copy

import SimpleBO

# The main program that executes. This call creates an instance of a
# class and the constructor starts the runtime.
app = Flask(__name__)


def parse_and_print_args():

    fields = None
    in_args = None
    if request.args is not None:
        in_args = dict(copy.copy(request.args))
        fields = copy.copy(in_args.get('fields', None))
        teamid = copy.copy(in_args.get('teamid', None))
        yearid = copy.copy(in_args.get('yearid', None))

        if fields:
            del(in_args['fields'])
        if teamid:
            del (in_args['teamid'])
        if yearid:
            del(in_args['yearid'])
        if in_args.get('offset', None):
            del(in_args['offset'])
        if in_args.get('limit', None):
            del(in_args['limit'])

    try:
        if request.data:
            body = json.loads(request.data)
        else:
            body = None
    except Exception as e:
        print("Got exception = ", e)
        body = None

    print("Request.args : ", json.dumps(in_args))

    return in_args, fields, teamid[0] if teamid else None, yearid[0] if yearid else None, body


def get_variable_from_path(path):

    variable_idx = path.find('&')
    if variable_idx == -1:
        return path
    else:
        return path[:variable_idx]


def get_limit_and_offset():
    offset_idx = request.url.find('&offset=')
    limit_idx = request.url.find('&limit=')
    if limit_idx == -1:
        limit = '10'
    else:
        limit = request.url[limit_idx+7:]
    if offset_idx == -1:
        offset = '0'
    else:
        offset = request.url[offset_idx+8:limit_idx]

    return limit, offset


def generate_links(result, offset, new_offset):

    d = {}
    d['data'] = result
    old_url = request.url
    idx = old_url.find('&offset')
    if idx == -1:
        old_url = old_url + '&offset=0&limit=10'
        new_url = request.url + '&offset=' + str(new_offset) + '&limit=10'
    else:
        new_url = request.url.replace("offset=" + str(offset), "offset=" + str(new_offset))

    d['links'] = [{'current':old_url}, {'next': new_url}]

    return d


@app.route('/api/<resource>', methods=['GET', 'POST'])
def get_resource(resource):

    in_args, fields, _, _, body = parse_and_print_args()
    if request.method == 'GET':
        limit, offset = get_limit_and_offset()
        result = SimpleBO.find_by_template(resource, in_args, fields, limit, offset)
        new_offset = len(result)
        print result
        result_with_links = generate_links(result, offset, new_offset)
        return json.dumps(result_with_links), 200, \
               {"content-type": "application/json; charset: utf-8"}

    elif request.method == 'POST':
        print body
        SimpleBO.insert_row(resource, body)
        return "End of base resource post request"

    else:
        return "Method " + request.method + " on resource " + resource + \
               " not implemented!", 501, {"content-type": "text/plain; charset: utf-8"}


@app.route('/api/<resource>/<primary_key>', methods=['GET', 'PUT', 'DELETE'])
def get_specific_resource(resource, primary_key):

    in_args, fields, _, _, body = parse_and_print_args()

    if request.method == 'GET':
        limit, offset = get_limit_and_offset()
        result = SimpleBO.find_by_primary_key(resource, primary_key, fields, limit, offset)
        new_offset = len(result)
        print result
        result_with_links = generate_links(result, offset, new_offset)
        return json.dumps(result_with_links), 200, \
               {"content-type": "application/json; charset: utf-8"}

    elif request.method == 'PUT':
        if not body:
            return "No update needed"
        SimpleBO.update_row(resource, primary_key, body)

    elif request.method == 'DELETE':

        SimpleBO.delete_row(resource, primary_key)

    return "End of get specific resource"


@app.route('/api/<resource>/<primary_key>/<related_resource>', methods=['GET', 'POST'])
def get_dependent_resource(resource, primary_key, related_resource):

    print 'this route'

    if '/career_stats' in request.url:
        limit, offset = get_limit_and_offset()
        result = SimpleBO.find_career_stats(primary_key, limit, offset)
        new_offset = len(result)
        print result
        result_with_links = generate_links(result, offset, new_offset)
        return json.dumps(result_with_links), 200, \
               {"content-type": "application/json; charset: utf-8"}

    else:
        in_args, fields, _, _, body = parse_and_print_args()
        limit, offset = get_limit_and_offset()
        print in_args
        print fields
        if request.method == 'GET':
            try:
                result = SimpleBO.find_related_rows(resource, primary_key, related_resource, in_args, fields, limit,\
                                                    offset)
                new_offset = len(result)
                print result
                result_with_links = generate_links(result, offset, new_offset)
                return json.dumps(result_with_links), 200, \
                       {"content-type": "application/json; charset: utf-8"}

            except Exception as e:
                print("Got exception = ", e)

        elif request.method == 'POST':
            try:
                SimpleBO.update_related_row(resource, primary_key, related_resource, body)
                return "Creation of new related resource succeeded"
            except Exception as e:
                print("Got exception = ", e)
                return "End of creating new related resource"

        else:

            return "End of getting dependent resource"


@app.route('/api/teammates/<playerid>', methods=['GET'])
def get_teammates(playerid):
    print playerid
    variable = get_variable_from_path(playerid)
    print variable
    limit, offset = get_limit_and_offset()
    result = SimpleBO.find_teammates(variable, limit, offset)
    new_offset = len(result)
    print result
    result_with_links = generate_links(result, offset, new_offset)

    return json.dumps(result_with_links), 200, \
           {"content-type": "application/json; charset: utf-8"}


@app.route('/api/roster', methods=['GET'])
def get_roster_and_states():

    _, _, teamid, yearid, _ = parse_and_print_args()
    limit, offset = get_limit_and_offset()
    result = SimpleBO.find_roster_stats(teamid, yearid, limit, offset)
    new_offset = len(result)
    print result
    result_with_links = generate_links(result, offset, new_offset)
    return json.dumps(result_with_links), 200, \
           {"content-type": "application/json; charset: utf-8"}


if __name__ == '__main__':
    app.run()

