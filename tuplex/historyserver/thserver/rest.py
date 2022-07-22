#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#


from thserver import app, socketio, mongo, MONGO_URI
from thserver.database import *
from thserver.config import *
from thserver.common import *
from thserver.version import __version__
from flask import render_template, request, abort, jsonify, make_response
import pymongo

import json
import os
import uuid
import sys
import datetime
import logging

from bson.objectid import ObjectId
# mongodb helpers
def normalize_from_mongo(q):
    def normalize_id(d):
        d['id'] = str(d['_id'])
        del d['_id']
        return d
    return [normalize_id(el) for el in q]


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

# test via curl -i http://localhost:5000/api/jobs
@app.route('/api/jobs', methods=['GET'])
def get_jobs():
    # retrieve jobs from mongodb
    # limit to 12 most recent jobs
    num_jobs = 25
    jobs = normalize_from_mongo(mongo.db.jobs.find({}, {'_id': 1,
                                                        'action' : 1,
                                                        'context': 1,
                                                        'status': 1,
                                                        'state_info': 1,
                                                        'progress': 1}).sort([('created', -1)]).limit(num_jobs))

    # Todo: create functions for handling/persisting dates/times better...
    # sort jobs after submitted time
    jobs = sorted(jobs, key=lambda x: string_to_utc(x['state_info']['submitted']), reverse=True)


    return jsonify(jobs)

@app.route('/api/job', methods=['GET'])
def get_job_request():
    job_id = request.args.get('id')

    if not job_id:
        return jsonify({'error': 'id field in request not included'}), 400

    job = Job(job_id).get()

    if not job:
        return jsonify({'error' : 'Could not find a job with id {}'.format(job_id)}), 404

    return jsonify(job), 201


def get_track_url(request, jobid):
    return '{}ui/job?id={}'.format(request.url_root, jobid)

# add a job
# test via curl -i -H "Content-Type: application/json" -X POST -d '{"title":"Read a book"}' http://localhost:5000/api/job
@app.route('/api/job', methods=['POST'])
def create_task():
    if not request.json:
        abort(400)

    jr = request.get_json()['job']
    print(jr)
    operators = request.get_json()['operators']
    job = Job() # creates new mongodb job

    # set_context(self, host, mode, name, user, conf, update=False):
    job.set_context(jr['context']['host'],
                    jr['context']['mode'],
                    jr['context']['name'],
                    jr['context']['user'],
                    jr['context']['config'])

    # set stages (i.e. physical plan info!)
    job.set_stages(jr['stages'])
    job.set_lastStageID(jr['lastStageId'])
    job.set_lastAction(jr['action'])

    # save all data to respective documents
    job.persist()

    # add each operator to operators collection
    operators = [{'idx' : idx, 'jobid' : job.jobid, 'ecount' : 0, 'ncount' : 0, **op} for idx, op in enumerate(operators)]
    mongo.db.operators.insert(operators)

    # notify all socketio clients
    msg = job.socketio_overview()
    print(msg)
    socketio.emit('newjob', msg)

    track_url = get_track_url(request, job.jobid)
    return jsonify({'job': {'id': job.jobid, 'track_url': track_url}}), 201

@app.route('/api/job/update', methods=['POST'])
def update_status():
    """
    update status of a job
    Returns:
    """
    if not request.json:
        abort(400)

    # try:
    data = request.get_json()
    status = {}
    status['jobid'] = data['jobid']
    status['status'] = data['status'] # can be scheduled, started, running, finished
    if 'progress' in data:
        status['progress'] = data['progress']
    status['time'] = current_utc_string()

    duration = 0.0
    state_info = mongo.db.jobs.find_one({'_id': ObjectId(data['jobid'])},
                                        {'_id' : 0, 'state_info' : 1})['state_info']
    if state_info and 'started' in state_info:
        started_dt = state_info['started']

        # duration is in s
        duration = (datetime.datetime.now(datetime.timezone.utc) - string_to_utc(started_dt)).total_seconds()
    status['state_info'] = state_info
    status['state_info']['duration'] = duration
    status['state_info'][data['status']] = status['time']

    # update stateinfo depending on status
    # write to mongodb
    if 'progress' in data:
        mongo.db.jobs.update_one({'_id': ObjectId(status['jobid'])},
                                 {'$set': {'status': data['status'],
                                           'progress': data['progress'],
                                           'state_info.' + data['status']: status['time'],
                                           'state_info.duration': duration}})
    else:
        mongo.db.jobs.update_one({'_id': ObjectId(status['jobid'])},
                                 {'$set': {'status': data['status'],
                                           'state_info.' + data['status']: status['time'],
                                           'state_info.duration': duration}})


    # send status update to all socketio clients
    socketio.emit('status', status)

    # sent job status
    return jsonify(status)

@app.route('/api/task', methods=['POST'])
def update_task():
    """
    updates summary info for task, i.e. how many normal case integers occurred, which etc.
    Returns:
    """
    if not request.json:
        abort(400)

    # @TODO: put this into Job interface??

    js = request.get_json()
    jobid = js['jobid']
    stageid = int(js['stageid'])
    ncount_delta = js['ncount'] # how many normal tuples have been processed in this task?
    ecount_delta = js['ecount'] # how many exceptions were thrown?

    # save to mongodb
    mongo.db.jobs.update_one({'_id': ObjectId(jobid), 'stages.stageid': stageid},
                             {'$inc': {'stages.$.ncount': ncount_delta, 'stages.$.ecount': ecount_delta}})
    mongo.db.jobs.update_one({'_id': ObjectId(jobid)},
                             { '$inc': { 'ncount': ncount_delta, 'ecount' :  ecount_delta }})
    # query full values
    status = mongo.db.jobs.find_one({'_id': ObjectId(jobid)},
                                    {'_id': 0, 'ncount': 1, 'ecount': 1})
    status['jobid'] = jobid

    print('/api/task:\n{}'.format(status))
    # send status update to all socketio clients
    socketio.emit('task_status', status)

    return jsonify({'status': 'ok'})

@app.route('/api/plan', methods=['POST'])
def update_plan():
    """
    stores plan info for physical plan page
    Returns:
    """
    if not request.json:
        abort(400)

    try:
        js = request.get_json()

        jobid = js['jobid']

        ir = {'optimizedIR' : js['stage']['optimizedIR'], 'unoptimizedIR' : js['stage']['unoptimizedIR']}

        # add plan to json dict
        job = Job(jobid)
        job.set_plan(ir)

        return jsonify({'status' : 'ok'})
    except Exception as e:
        return jsonify({'status' : 'error'})

@app.route('/api/operator', methods=['POST'])
def update_operator():
    """
    updates a single exception type for one op in one job
    Returns:
    """
    print('operator progress update request')
    if not request.json:
        abort(400)

    js = request.get_json()

    print(js)

    # update status
    jobid = js['jobid']
    opid = js['opid']
    ncount_delta = js['ncount']  # how many normal tuples have been processed in this task?
    ecount_delta = js['ecount']  # how many exceptions were thrown?
    detailed_ecounts = js['detailed_ecounts'] # counts per exception type

    print('detailed counts:')
    print(detailed_ecounts)
    # old
    # mongo.db.operators.update_one({'jobid': ObjectId(jobid), '_id': opid},
    #                               {'$inc': {'ncount': ncount_delta, 'ecount': ecount_delta}})

    inc_dict = {'ncount' : ncount_delta, 'ecount' : ecount_delta}
    for key, val in detailed_ecounts.items():
        inc_dict['detailed_ecounts.' + key] = val

    # upsert will create inc fields
    mongo.db.operators.update_one({'jobid': jobid, 'id' : opid},
                                  {'$inc': inc_dict},
                                  upsert=True)

    # fetch operator info
    status = mongo.db.operators.find_one({'jobid': ObjectId(jobid), 'id' : opid},
                                         {'_id': 0,
                                          'ncount': 1,
                                          'ecount': 1})

    assert status
    # query full and sent socketio update
    # send status update to all socketio clients
    status.update({'jobid' : jobid, 'opid' : opid})
    socketio.emit('operator_status', status)

    return jsonify({'status': 'ok'})


"""
This method gets a job from mongodb based on
the inputted jobid.
"""
def get_job(jobid):


    # check whether id is valid, else return None
    if not ObjectId.is_valid(jobid):
        return None

    # merge with operators belonging to this job
    # and sort after idx!
    res = list(mongo.db.jobs.aggregate([
        {'$match': {'_id' : ObjectId(jobid)}},
        { "$addFields": { "article_id": { "$toString": "$_id" }}},
        {'$lookup' : {
            'from' : 'operators',
            'localField' : 'article_id',
            'foreignField' : 'jobid',
            'as' : 'operators'
        }}, {'$sort' : {'idx' : 1}}
    ]))


    if 0 == len(res):
        return None

    assert len(res) <= 1
    job = res[0]

    # change ObjectId fields
    job['id'] = str(job['_id'])
    del job['_id']

    def clean_op(op):
        res = op.copy()
        res['jobid'] = str(res['jobid'])
        del res['_id']
        return res


    job['operators'] = [clean_op(op) for op in job['operators']]

    return job

@app.route('/api/operators', methods=['GET'])
def display_all_operators():
    res = normalize_from_mongo(mongo.db.operators.find({}))

    return jsonify(res)


@app.route('/api/operator', methods=['GET'])
def get_operator_details():
    """
    get details for operator
    Returns:
    """
    jobid = request.args.get('jobid')
    opid = request.args.get('opid')

    res = mongo.db.operators.find_one({'id': opid, 'jobid': jobid})
    if 'exceptions' in res:
        res['exceptions'] = sorted(res['exceptions'], key=lambda x: x['code'])
        # update exceptions nicely
        for exc in res['exceptions']:
            exc['count'] = res['detailed_ecounts'][exc['code']]
        res['opid'] = res['id']
        res['jobid'] = str(res['jobid'])
        # del res['_id']
        del res['detailed_ecounts']
        if not res:
            return jsonify({'error' : 'no result found for opid={} and jobid={}'.format(opid, jobid)})
    if 'exceptions' not in res and 'detailed_ecounts' in res:
        res['exceptions'] = []

        # get detailed_ecounts
        for key in sorted(res['detailed_ecounts'].keys()):
            res['exceptions'].append({'count': res['detailed_ecounts'][key], 'code': key})
    res['_id'] = str(res['_id'])
    return jsonify(res)

@app.route('/api/exception', methods=['POST'])
def update_exception():
    """
    updates a single exception type for one op in one job
    Returns:
    """
    print('exception (details) update request')
    if not request.json:
        abort(400)

    js = request.get_json()

    print(js)

    # request should look like this
    # {'jobid' : '...',
    #  'opid' : '...',
    #  'ncount' : 20,
    #  'ecount' : 12,
    #  'sample_column_names' : ['colA', ..., 'colZ'],
    #  'exceptions' : [{'code' : 3765,
    #  'count' : 8734,
    #  'first_row_traceback' : kdjg,
    #  'sample' : ??}, ...]}

    jobid = js['jobid']
    opid = js['opid']
    previous_operator_columns = js['sample_column_names']
    exceptions = js['exceptions']

    # compute ecount:
    ecount = 0

    for exception in exceptions:
        print('exception is: ')
        print(exception)
        assert 'code' in exception
        assert 'first_row_traceback' in exception
        assert 'sample' in exception
        assert 'count' in exception

    mongo.db.operators.update_one({'jobid': jobid, 'id': opid}, {'$set' : {'exceptions' : exceptions,
                                                                           'previous_operator_columns' : previous_operator_columns}})

    return jsonify({'status' : 'ok'})

@app.route('/api/version', methods=['GET'])
def get_info():
    client = mongo.cx
    # there's a bug in the ubuntu version, if no session has been started then
    # the address field is None. Therefore, start by retrieving server info a session
    info = client.server_info()
    nodes = client.nodes
    addr = mongo.cx.address
    if addr is None:
        if nodes is not None and len(nodes) > 0:
            nodes = list(nodes)
            addr = nodes[0]
            if not isinstance(addr, tuple) and len(addr) == 2:
                addr = (None, None)
        else:
            addr = (None, None)

    mongo_info = dict(zip(('host', 'port'), addr))
    mongo_info['uri'] = MONGO_URI

    return jsonify({'version': __version__,
                    'name': 'Tuplex WebUI',
                    'mongodb': mongo_info})

@socketio.on('message')
def handle_message(message):
    print('received message: ' + message)

@app.route('/api/clearjobs', methods=['POST'])
def clear_jobs():

    mongo.db.jobs.remove({})
    mongo.db.operators.remove({})
    print('cleared all jobs')

    # clear all jobs in overview + operators
    return jsonify({'status' : 'ok'})


@app.route('/api/stage/result', methods=['POST'])
def update_stage_result():
    if not request.json:
        abort(400)

    js = request.get_json()
    # layout is

    # set global normal / exceptional row count for a stage
    job = Job(js['jobid'])


    # i.e. update stats counters within stage in job AND for each operator.
    stageid = js['stageid']
    exceptionCounts = js['exceptionCounts']
    num_exception_rows = js['ecount']
    num_normal_rows = js['ncount']
    job.update_stage_counts(stageid, num_normal_rows, num_exception_rows, exceptionCounts)

    return jsonify({'status': 'ok'})
