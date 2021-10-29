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

# handle interactions with mongo db database (i.e. ORM)

from thserver import app, socketio, mongo
from thserver.config import *
from thserver.common import *
from thserver.version import __version__
from flask import render_template, request, abort, jsonify, make_response
import datetime

from bson.objectid import ObjectId

from functools import reduce

class Job:
    """
    this is a (shallow) wrapper around a job to perform various databasuy updates etc.
    """

    def __init__(self, jobid=None):
        """
        init job from database
        :param jobid:
        """
        if jobid is None:

            # create new empty job in database
            job = {}
            job['action'] = 'undefined'
            job['state_info'] = {'submitted': current_utc_string()}
            job['lastStageId'] = -1 # use to select ncounts/ecounts
            job['created'] = current_utc_timestamp()
            job['stages'] = []
            job['status'] = 'created'
            job['ncount'] = 0
            job['ecount'] = 0

            # retrieve id
            self._id = mongo.db.jobs.insert_one(job).inserted_id
            self.jobid = str(self._id)

        else:
            self._id = ObjectId(jobid)
            self.jobid = jobid


    def __str__(self):
        """
        string representation of this job
        :return:
        """

        return 'Job(id={})'.format(self.jobid)

    def set_context(self, host, mode, name, user, conf, update=False):
        """
        sets job configuration dictionary
        :param config:
        :return:
        """

        assert isinstance(conf, dict), 'conf should be a dictionary'

        # mongodb doesn't like . in keys. hence, update all the config keys
        escaped_conf = {}
        for key, val in conf.items():
            if '~' in key:
                raise ValueError('key "{}" contains ~ character. Not allowed.'.format(key))
            escaped_conf[key.replace('.', '~')] = val

        self.context = {'config' : escaped_conf,
                        'host' : host,
                        'mode' : mode,
                        'name' : name,
                        'user': user}

        # perform update in database if requested
        if update:
            # set in mongodb
            assert self._id, 'id must not be None'

            mongo.db.jobs.update_one({'_id': self._id},
                                     {'$set': {'context': self.context}})


    def set_stages(self, stages, update=False):
        # stages is a dictionary of individual stage json objects
        # i.e.
        # 'stages': [{'id': 1, 'operators': [{'columns': [], 'id': 'op100000', 'name': 'parallelize'}, {'columns': [], 'id': 'op100001', 'name': 'map', 'udf': 'lambda a, b : a / b'}, {'columns': [], 'id': 'op100002', 'name': 'collect'}], 'predecessors': []}]

        # break up into separate operator table for live updates on individual operators, just keep logical structure here i.e.

        self.operators = []
        self.stages = []
        for stage in stages:

            # add empty count stages here
            self.stages.append({'stageid' : stage['id'], 'ncount' : 0, 'ecount' : 0, 'predecessors': stage["predecessors"]})

            if 'operators' in stage.keys():
                operators = stage['operators']

                # add each operator to operators collection
                # ncount for a stage is same across all operators
                self.operators += [{'idx' : idx,
                                    'jobid' : self._id,
                                    'stageid' : stage['id'],
                                    'ecount' : 0,
                                    'ncount' : 0,
                                    **op} for idx, op in enumerate(operators)]

        if update:
            mongo.db.operators.insert(self.operators)
            del self.operators

            mongo.db.jobs.update_one({'_id': self._id}, {'$set': {'stages': self.stages}})

    def set_plan(self, ir):
        # insert into mongo for job
        return mongo.db.jobs.update_one({'_id': self._id}, {'$set': {'plan': ir}})


    def set_lastStageID(self, id, update=False):

        self.lastStageId = id

        if update:
            return mongo.db.jobs.update_one({'_id': self._id}, {'$set': {'lastStageId': self.lastStageId}})

    def set_lastAction(self, action, update=False):
        self.action = action

        if update:
            return mongo.db.jobs.update_one({'_id': self._id}, {'$set': {'action': self.action}})

    def persist(self):
        """
        write current state of this class to database
        :return:
        """

        set_dict = {}

        # lazy set multiple vars in one query...
        if self.context:
            set_dict['context'] = self.context
            del self.context

        if self.stages:
            set_dict['stages'] = self.stages
            del self.stages

        if self.lastStageId:
            set_dict['lastStageId'] = self.lastStageId
            del self.lastStageId

        if self.action:
            set_dict['action'] = self.action
            del self.action

        # put operators in separate document...
        if self.operators:
            mongo.db.operators.insert(self.operators)
            del self.operators


        mongo.db.jobs.update_one({'_id': self._id},
                                 {'$set': set_dict})

    def get_job_conf(self):
        # check whether id is valid, else return None
        if not ObjectId.is_valid(self.jobid):
            return None

        escaped_conf = mongo.db.jobs.find_one({'_id': self._id})['context']['config']

        conf = {}
        # replace ~ to . in keys
        for k, v in escaped_conf.items():
            conf[k.replace('~', '.')] = v

        return conf

    def get(self):
        """
        returns full data representation of a job
        :return:
        """

        # check whether id is valid, else return None
        if not ObjectId.is_valid(self._id):
            return None

        # merge with operators belonging to this job
        # and sort after idx!
        res = list(mongo.db.jobs.aggregate([
            {'$match': {'_id': self._id}},
            {'$lookup': {
                'from': 'operators',
                'localField': '_id',
                'foreignField': 'jobid',
                'as': 'operators'
            }}, {'$sort': {'idx': 1}}
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


        # fetch ecount / ncount from last Stage Id!
        ncount, ecount = 0, 0
        for stage in job['stages']:
            if stage['stageid'] == job['lastStageId']:
                ncount = stage['ncount']
                ecount = stage['ecount']
                break

        job['ncount'], job['ecount'] = ncount, ecount

        return job

    def socketio_overview(self):

        job = self.get()

        return {'context': job['context'],
                'status': job['status'],
                'state_info': job['state_info'],
                'action': job['action'],
                'id': job['id']}

    def update_stage_counts(self, stageid, num_normal_rows, num_exception_rows, exceptionCounts):

        ncount = num_normal_rows  # important
        ecount = 0

        # aggregate counts + set them
        if num_exception_rows > 0 and len(exceptionCounts) >  0:
            # aggregate exception counts for operator...
            total_ecounts = 0
            for info in exceptionCounts:
                #[{'class': 'ZeroDivisionError', 'code': 136, 'count': 2, 'opid': 100001}]
                set_dict = {'ecount': info['count']}
                total_ecounts += info['count']
                mongo.db.operators.update_one({'jobid': self._id, 'stageid' : stageid, 'idx' : info['idx']},
                                              {'$set': set_dict})

            assert num_exception_rows == total_ecounts, 'numbers are not matching'

            # compute normal / exception count for job across all stages
            # aggregate query to figure out total ncount AND ecount for a job
            grouped_stage_counts = list(mongo.db.operators.aggregate([{'$match': {'jobid': self._id}},
                                                                      {'$group': {'_id': '$stageid',
                                                                                  'ecount': {'$sum': '$ecount'}}},
                                                                      {'$project': {'stageid': '$_id', '_id': False,
                                                                                    'ecount': True}}]))
            ecount = reduce(lambda a, b: a['ecount'] + b['ecount'], grouped_stage_counts, {'ecount': 0})


        # update counts for stage id on job
        mongo.db.jobs.update_one({'_id': self._id, 'stages.stageid' : stageid},
                                 {'$set': {'stages.$.ecount': ecount, 'stages.$.ncount': ncount}})
