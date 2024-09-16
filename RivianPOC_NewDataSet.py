#!/usr/bin/env python

'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!   NOTE: SCRIPT NEEDS TO BE COMPATIBLE WITH PYPY3!
!   THIS SAMPLE IS BUILT USING MIMESIS 11.1.0.
!   IF YOU ARE USING A SCRIPT THAT USES AN OLDER VERSION,
!   YOU NEED TO EITHER UPGRADE YOUR CODE TO MATCH THIS TEMPLATE
!   OR GO INTO THE REQUIREMENTS FILE AND CHANGE THE MIMESIS VERSION
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
'''

########################################################################
# 
# This is an example Locust file that use Mimesis to help generate
# dynamic documents. Mimesis is more performant than Faker
# and is the recommended solution. After you build out your tasks,
# you need to test your file in mLocust to confirm how many
# users each worker can support, e.g. confirm that the worker's CPU
# doesn't exceed 90%. Once you figure out the user/worker ratio,
# you should be able to figure out how many total workers you'll need
# to satisfy your performance requirements.
#
# These Mimesis locust files can be multi-use, 
# saturating a database with data or demonstrating standard workloads.
#
########################################################################

# Allows us to make many pymongo requests in parallel to overcome the single threaded problem
import gevent
_ = gevent.monkey.patch_all()

########################################################################
# TODO Add any additional imports here.
# TODO But make sure to include in requirements.txt
########################################################################
import pymongo
from bson import json_util
from bson.json_util import loads
from bson import ObjectId
from bson.decimal128 import Decimal128
from locust import User, events, task, constant, tag, between, runners
import time
from pickle import TRUE
from datetime import datetime, timedelta
import random
from decimal import Decimal
import string
from mimesis import Field, Fieldset, Schema
from mimesis.enums import Gender, TimestampFormat
from mimesis.locales import Locale
import json

# Global vars
# We can use this var to track the seq index of the worker in case we want to use it for generating unique seq keys in mimesis
_WORKER_ID = None
# Store the client conn globally so we don't create a conn pool for every user
# Track the srv globally so we know if we need to reinit the client
_CLIENT = None
_SRV = None
# Track the full host path so we know if anything changes
_HOST = None

@events.init.add_listener
def on_locust_init(environment, **_kwargs):
    global _WORKER_ID
    if not isinstance(environment.runner, runners.MasterRunner):
        _WORKER_ID = environment.runner.worker_index

# Mimesis global vars
# TODO Change the locale if needed
_ = Field(locale=Locale.EN)
_FS = Fieldset(locale=Locale.EN)
_SCHEMA = None

class MetricsLocust(User):
    ########################################################################
    # Class variables. 
    # The values are initialized with None
    # till they get set from the actual locust exeuction 
    # when the host param is passed in.
    # DO NOT HARDCODE VARS! PASS THEM IN VIA HOST PARAM.
    # TODO Do you have more than 20 tasks? If so, change the array init below.
    ########################################################################
    client, coll, bulk_size = None, None, None

    def __init__(self, parent):
        global _, _FS, _SCHEMA, _WORKER_ID, _HOST, _CLIENT, _SRV

        super().__init__(parent)

        try:
            vars = self.host.split("|")
            srv = vars[0]
            print("SRV:",srv)

            isInit = (_HOST != self.host)
            if isInit:
                print("Initializing...")
                self.client = pymongo.MongoClient(srv)
                _CLIENT = self.client
                _SRV = srv
                _HOST = self.host
            else:
                self.client = _CLIENT

            db = self.client[vars[1]]
            self.coll = db[vars[2]]

            # docs to insert per batch insert
            self.bulk_size = int(vars[3])
            print("Batch size from Host:",self.bulk_size)

            # init schema once
            if isInit:
                ########################################################################
                # mimesis schema for bulk creation
                # The zoneKey is a great way to demonstrate zone sharding,
                # e.g. all docs created by worker1 goes into shard1
                # and all docs created by worker2 goes into shard2
                # Note that increment doesn't maintain unique sequence numbers 
                # if you are running multiple mlocust users in parallel on the same worker
                # Not every api func has been used. The full api can be found here. https://mimesis.name/en/master/api.html
                # TODO Only use what you need. The more logic you have the slower your schema generation will be.
                ########################################################################
                # TODO modify how much you want to offset the increment by using the worker id
                # BUILT USING MIMESIS 17.0.0 to get rid of offensive words.
                _SCHEMA = Schema(
                    schema=lambda: {
                            "notification_id": _("random.generate_string_by_mask", mask="##@@#@##@@@#-#@##@-#@@@@@#", char="@", digit="#"),
                            "created_at": _("datetime.datetime", start=2000, end=2023),
                            "destination": _("random.weighted_choice", choices={'test-CHKOUT-a00e3789-f9d0-4d7b-8f15-fbbc2d3db651':20, 'test-RVN-rivian_id':20, 'lhamiltonQA@rivian.com':20, '02-8aad2728-fa06-49c8-973c-53929f883a8d-c5e41b81':20, 'rivian.test.user+a6b37c39@gmail.com':20}),
                            "error_messages": json.loads(_("random.weighted_choice", choices={'[{\"S\":\"Rivian User or User Email Not Found for rivian_id : 02-2f770b19-2461-4819-b2dc-2c5110281c80-8927fe12\"}]':20, '[{\"S\":\"Rivian User or User Email Not Found for rivian_id : test-RVN-b225af73-83ed-49fa-823e-504a57e9c793\"}]':20, '[{\"S\":\"Rivian User or User Email Not Found for rivian_id : test-RVN-121ffb50-319a-4782-a43c-d6a58f525c4f\"}]':20, '[{\"S\":\"Message not sent - User communication disabled communication for: all_channels or available notification channels overriden by to_destination in request_body\"}]':20, '[{\"S\":\"Rivian User or User Email Not Found for rivian_id : test-RVN-dc204c34-6b9c-4cdc-80b6-52ae8aee8518\"}]':20})),
                            "event_bus_events": _("random.weighted_choice", choices={'':100}),
                            "events": json.loads(_("random.weighted_choice", choices={'[{\"M\":{\"event_id\":{\"S\":\"shardId-000000000002:49652404840168690355104344190611565194529860380161736738\"},\"event_date\":{\"S\":\"2024-06-05T23:58:06.436000\"},\"event_type\":{\"S\":\"DELIVERED\"}}}]':20, '[{\"M\":{\"event_id\":{\"S\":\"shardId-000000000002:49622112451658543158502130009957927904525492410776551458\"},\"event_date\":{\"S\":\"2021-12-21T08:13:42.431000\"},\"event_type\":{\"S\":\"SEND\"}}},{\"M\":{\"event_id\":{\"S\":\"shardId-000000000003:49622112451680843903700660663269416377100828242956255282\"},\"event_date\":{\"S\":\"2021-12-21T08:13:42.431000\"},\"event_type\":{\"S\":\"DELIVERED\"}}}]':20, '[{\"M\":{\"event_id\":{\"S\":\"shardId-000000000003:49629168152341133921614107346267696987727495747468263474\"},\"event_type\":{\"S\":\"INVALID\"},\"sms_secondary_event_type\":{\"S\":\"FAILURE\"},\"event_date\":{\"S\":\"2022-05-06T19:10:17.936000\"}}}]':20, '[{\"M\":{\"event_date\":{\"S\":\"2021-04-23T06:03:57.356163\"},\"event_type\":{\"S\":\"FAILURE\"}}}]':20, '[{\"M\":{\"event_id\":{\"S\":\"shardId-000000000005:49627429116368755141199678400321490024471148700998041682\"},\"event_type\":{\"S\":\"INVALID\"},\"sms_secondary_event_type\":{\"S\":\"FAILURE\"},\"event_date\":{\"S\":\"2022-04-26T22:03:16.343000\"}}}]':20})),
                            "message_id": _("random.generate_string_by_mask", mask="##@@#@##-##@#-#@##-#@##-##@#@#@##@@#", char="@", digit="#"),
                            "message_name": _("random.weighted_choice", choices={'vehicle_fault_propagated_thermal_event':20, 'drivers_keys_confirm_removal_co_user':20, 'notifications_opt_in':20, 'garage_short_term_vehicle_request_created_updated_coordinator':20, 'alarm_disabled_push':20}),
                            "message_type": _("random.weighted_choice", choices={'all':20, 'EMAIL':20, 'SMS':20, 'GCM':20, 'APNS':20}),
                            "provider_app_id": _("random.weighted_choice", choices={'9054f11bb15945288f4f82ac40bffff6':50, '5c33b2e607bc434e829ce07e714b76a4':50}),
                            "provider_id": _("random.weighted_choice", choices={'02000000j945ovpl-q91gh0u3-3ni1-7cg2-39a0-1u53qeb04og0-000000':20, '02000000l3f251v6-0khjgnjs-ih61-665f-5qoh-kvcg052d0t80-000000':20, 'usvqjp3a4r33m3pcocnq4lnb2s82srqdc4ibtd80':20, 'APNS-add6292f-bcb9-4692-b138-c3a789db3f9a':20, 'vh0hqikaq6r2hpclnhs1s0dtitp8hdbufsta0hg0':20}),
                            "rivian_id": _("random.weighted_choice", choices={'02-3917f957-f488-44ef-bfcc-fde5a8582d31-0be9780f':20, 'test-RVN-5633d7e5-c2d5-42a0-95f5-943265020713':20, '02-90d67d67-a912-4b95-8ec4-4e565a1f436a-7e51ca89':20, '02-02b72b13-6f2a-443b-bb08-4aa87081f3f9-10e70b8f':20, '02-0b22aedd-eb8c-41ab-b24b-468e31a126bb-12ee17b6':20}),
                            "status": _("random.weighted_choice", choices={'INTERNAL_ERROR':10, 'DELIVERY':10, 'OPEN':10, 'SENT':10, 'BOUNCE':10, 'INVALID':10, 'FAILURE':10, 'SENT_WITH_ERROR':10, 'SENT_TO_USER':20}),
                            "updated_at": _("datetime.datetime", start=2000, end=2023),
                    },
                    iterations=self.bulk_size
                )
        except Exception as e:
            # If an exception is caught, Locust will show a task with the error msg in the UI for ease
            events.request.fire(request_type="Host Init Failure", name=str(e), response_time=0, response_length=0, exception=e)
            raise e

    ################################################################
    # Example helper function that is not a Locust task.
    # All Locust tasks require the @task annotation
    ################################################################
    def get_time(self):
        return time.time()

    ################################################################
    # Since the loader is designed to be single threaded with 1 user
    # There's no need to set a weight to the task.
    # Do not create additional tasks in conjunction with the loader
    # If you are testing running queries while the loader is running
    # deploy 2 clusters in mLocust with one running faker and the
    # other running query tasks
    # The reason why we don't want to do both loads and queries is
    # because of the simultaneous users and wait time between
    # requests. The bulk inserts can take longer than 1s possibly
    # which will cause the workers to fall behind.
    ################################################################
    @task(1)
    def _bulkinsert(self):
        global _SCHEMA 

        # Note that you don't pass in self despite the signature above
        tic = self.get_time();
        name = "bulkInsert";
 
        try:
            # If you want to do an insert_one, you need to grab the first array element of schema, e.g. (schema*1)[0]
            self.coll.insert_many(_SCHEMA.create(), ordered=False)

            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0)
        except Exception as e:
            events.request.fire(request_type="mlocust", name=name, response_time=(self.get_time()-tic)*1000, response_length=0, exception=e)
            # Add a sleep so we don't overload the system with exceptions
            time.sleep(5)
