from thserver import app
from thserver.database import *
from thserver.config import *
from thserver.common import current_utc_string, string_to_utc
from thserver.rest import get_jobs, get_job
from thserver.version import __version__
from flask import render_template, request, abort, jsonify, make_response

import json
import os
import uuid
import sys

import dateutil.tz as tz
from datetime import datetime, tzinfo, timedelta


def suffix(d):
    return 'th' if 11 <= d <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(d % 10, 'th')


def custom_strftime(format, t):
    return t.strftime(format).replace('{S}', str(t.day) + suffix(t.day))


@app.template_filter('strftime')
def _jinja2_filter_datetime(date, fmt=None):
    if len(date) == 0:
        return ''

    date = string_to_utc(date)
    to_zone = tz.tzlocal()

    class Zone(tzinfo):
        def __init__(self, offset, isdst, name):
            self.offset = offset
            self.isdst = isdst
            self.name = name

        def utcoffset(self, dt):
            return timedelta(hours=self.offset) + self.dst(dt)

        def dst(self, dt):
            return timedelta(hours=1) if self.isdst else timedelta(0)

        def tzname(self, dt):
            return self.name

    # UTC is GMT zone...
    GMT = Zone(0, False, 'GMT')
    utc = date.replace(tzinfo=GMT)
    local = utc.astimezone(to_zone)

    # format='%b %d %H:%M:%S'
    format = '%b %d, %H:%M:%S'
    # return native.strftime(format)

    format = '%b {S}, %H:%M:%S'
    return custom_strftime(format, local)


@app.template_filter('humanizetime')
def _jinja2_filter_humanizetime(dt, fmt=None):
    """
    humanizes time in seconds to human readable format
    Args:
        dt:
        fmt:
    Returns:
    """

    days = int(dt / 86400)
    hours = int(dt / 3600) % 24
    minutes = int(dt / 60) % 60
    seconds = int(dt) % 60
    microseconds = int(((dt % 1.0) * 1000))

    fmtstr = ''
    if days > 0:
        fmtstr += '{}d '.format(days)
    if hours > 0:
        fmtstr += '{}h '.format(hours)
    if minutes > 0:
        fmtstr += '{}m '.format(minutes)
    if seconds > 0:
        fmtstr += '{}s '.format(seconds)
    if microseconds > 0:
        # special case: only include ms for times below a minute.
        if minutes == 0 and hours == 0 and days == 0:
            fmtstr += '{}ms '.format(microseconds)
    return fmtstr.rstrip()


# idea is pretty simple. urls of the form /api/... provide REST interface
# /ui/... is the same but with nicely visualized webui


# endpoints:
# multi context
# /api/apps             list all available applications
# /api/app              get/add application
# /api/jobs?app=id      list jobs for application with appid id

# /api/jobs             list all available, stored jobs
# /api/job/id           get details for job with id
# /api/job              add job with some data
# /api/

@app.route('/')
@app.route('/ui')
@app.route('/ui/jobs')
def index():
    # job list is as follows:
    # [
    # Action	Status	User	Context	Uptime	Submitted	Progress
    # ]
    # jobs = [{'action' : 'collect', 'status' : 'running',
    #          'user' : 'leonhards', 'context' : 'context0',
    #          'submitted' : current_timestamp(), 'started' : current_timestamp(), 'finished' : '',
    #          'progress' : '100/400 tasks (25%)', 'id' : str(uuid.uuid4())},
    #         {'action': 'collect', 'status': 'finished',
    #          'user': 'leonhards', 'context': 'context0',
    #          'submitted': current_timestamp(), 'started': current_timestamp(), 'finished': current_timestamp(),
    #          'progress': '100/400 tasks (25%)', 'id': str(uuid.uuid4())}
    #         ]

    # perform REST request to get jobs...
    jobs = get_jobs().json

    return render_template('overview.html', version=__version__, num_jobs=len(jobs), jobs=jobs)


@app.route('/ui/config', methods=['GET'])
def show_config():
    job_id = request.args.get('id')

    # fetch job from mongo db
    job = Job(job_id)

    # fetch job from mongo db
    conf = job.get_job_conf()

    if not conf:
        # show job not found page
        return render_template('job_not_found.html',version=__version__, jobid=job_id)

    return render_template("job_config.html", version=__version__, id=job_id, conf=conf)


@app.route('/ui/plan', methods=['GET'])
def showplan():
    job_id = request.args.get('id')

    # fetch job from mongo db
    job = Job(job_id).get()

    if not job:
        # show job not found page
        return render_template('job_not_found.html', version=__version__, jobid=job_id)

    print(job['plan']['unoptimizedIR'])

    # update line break to html friendly version
    # job['plan']['optimizedIR'] = job['plan']['optimizedIR'].replace('\n', '<br>')
    # job['plan']['unoptimizedIR'] = job['plan']['unoptimizedIR'].replace('\n', '<br>')


    print(job.keys())
    kwargs = {'version': __version__,
              'id': job_id,
              'plan' : job['plan']}

    return render_template('job_plan.html', **kwargs)

def fix(a):
    ret = ""
    for b in a:
        ret += "Stage " + str(b) + ","
    return ret[:-1]

@app.route('/ui/job', methods=['GET'])
def showjob():

    job_id = request.args.get('id')

    # fetch job from mongo db
    # job = Job(job_id).get()
    job = get_job(job_id)

    if not job:
        # show job not found page
        return render_template('job_not_found.html', version=__version__, jobid=job_id)

    # define styling of operators here
    opcssclass = {'parallelize': 'input',
                  'csv': 'input',
                  'take': 'action',
                  'collect': 'action',
                  'map': 'map',
                  'withColumn': 'map',
                  'mapColumn': 'map',
                  'selectColumns': 'map',
                  'resolve': 'resolve',
                  'filter': 'filter',
                  'aggregate_by_key': 'aggregate',
                  'aggregate': 'aggregate',
                  'join': 'join',
                  'left_join': 'join'}

    operators = job['operators']

    # sort operators into stages!
    stages = {}

    for op in operators:

        if 'detailed_ecounts' in op:

            # artifically add exception array if missing
            if 'exceptions' not in op:
                op['exceptions'] = []

                # get detailed_ecounts
                for key in sorted(op['detailed_ecounts'].keys()):
                    op['exceptions'].append({'count' : op['detailed_ecounts'][key], 'code' : key})
            else:
                # # for each detailed count update
                # for exc_name, count in op['detailed_ecounts'].items():
                for j, exc in enumerate(op['exceptions']):
                    if exc['code'] in op['detailed_ecounts']:
                        op['exceptions'][j]['count'] = op['detailed_ecounts'][exc['code']]

            del op['detailed_ecounts']

        if not op['stageid'] in stages:
            stages[op['stageid']] = []
        stages[op['stageid']].append(op)

    stage_info = {}
    for stage in job['stages']:
        stage_info[stage['stageid']] = stage

    # sort stages after number AND sort ops within after index
    sorted_stages = []
    for i in sorted(stages.keys()):

        # find stage in job['stages']
        if i in stage_info.keys():
            ncount = stage_info[i]['ncount']
            # ecount = stage_info[i]['ecount']
            ecount = 0
            a = stages[i]
            for idx, _ in enumerate(stages[i]):
                ecount += stages[i][idx]['ecount']
            sorted_stages.append({'number': i,
                                  'operators': list(sorted(stages[i], key=lambda op: op['idx'])),
                                  'ncount': ncount, 'ecount': ecount})
            if i != 0:
                sorted_stages[i]['dependencies'] = fix(stage_info[i]["predecessors"])
        else:
            sorted_stages.append({'number' : i,
                                  'operators' : list(sorted(stages[i], key=lambda op: op['idx']))})

    kwargs = {'version': __version__,
              'ncount': job['ncount'],
              'ecount': job['ecount'],
              'status': job['status'],
              'stages': sorted_stages,
              'opcssclass': opcssclass,
              'id': job_id}
    # duration?
    if 'duration' in job['state_info']:
        kwargs['duration'] = job['state_info']['duration']
    if 'started' in job['state_info']:
        kwargs['started'] = job['state_info']['started']

    # print(json.dumps(kwargs, indent=4, sort_keys=True))

    return render_template('job.html', **kwargs)

# this shouldn't be used in production...
# @app.route('/api/shutdown')
# def shutdown():
#     print('exiting history server...')
#
#     import sys, errno
#     sys.exit(errno.EINTR)
#
#     func = request.environ.get('werkzeug.server.shutdown')
#     if func is None:
#         raise RuntimeError('Not running with the Werkzeug Server')
#     func()
#     return 'History server successfully shutdown.'

# @app.route('/addjob', methods=['POST'])
# def addjob():
#     # content = request.get_json(silent=True)
#     # return content
#     return 'hello world'
#
# @app.route('/job/<jobid>')
# def job(jobid):
#     return 'This page will show contents for job page {}'.format(jobid)

# links: https://flask-pymongo.readthedocs.io/en/latest/
# https://blog.miguelgrinberg.com/post/designing-a-restful-api-with-python-and-flask
# https://flask-rest-jsonapi.readthedocs.io/en/latest/quickstart.html#first-example
# https://www.moesif.com/blog/technical/restful/Guide-to-Creating-RESTful-APIs-using-Python-Flask-and-MongoDB/
# https://www.bogotobogo.com/python/MongoDB_PyMongo/python_MongoDB_pyMongo_tutorial_connecting_accessing.php

# look at https://spark.apache.org/docs/latest/monitoring.html for some inspiration