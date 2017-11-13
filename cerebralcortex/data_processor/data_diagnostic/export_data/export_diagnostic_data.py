# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import os
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.configuration import Configuration
from datetime import timedelta


#create and load CerebralCortex object and configs
configuration_file = os.path.join(os.path.dirname(__file__), '../../../../cerebralcortex.yml')
CC_driver = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", load_spark=True)
CC_worker = CerebralCortex(configuration_file, master="local[*]", name="Data Diagnostic App", load_spark=False)

#output folder path
output_folder = "/home/ali/Desktop/DUMP/data/"

# load data diagnostic configs
config = Configuration(filepath="../data_diagnostic_config.yml").config

def one_participant_data(participant_ids=None):
    # get all streams for a participant
    if len(participant_ids)>0:
        participants_rdd = CC_driver.sc.parallelize(participant_ids)
        results = participants_rdd.map(
            lambda participant: export_data(participant, CC_worker, config))
        results.count()
    else:
        print("No participant is selected")


def all_participants_data(study_name):
    # get all participants' name-ids
    participants = CC_driver.get_all_participants(study_name)

    if len(participants)>0:
        participants_rdd = CC_driver.sc.parallelize(participants)
        results = participants_rdd.map(
            lambda participant: export_data(participant["identifier"], CC_worker, config))
        results.count()
    else:
        print("No participant is selected")


def export_data(participant_id, CC, config):
    streams = CC.get_participant_streams(participant_id)
    if streams and len(streams)>0:
        stream_names = filter_stream_names(streams)
        for key,val in stream_names.items():
            days = val["end_time"]-val["start_time"]
            for day in range(days.days+1):
                day_date = (val["start_time"]+timedelta(days=day)).strftime('%Y%m%d')
                data = CC.get_cassandra_raw_data(val["identifier"], day_date)
                write_to_bz2(output_folder+val["owner"]+"/", key+".csv", data)
                print("done")

def filter_stream_names(stream_names):
    filtered_names = {}
    for key, val in stream_names.items():
        if key.startswith('DATA-DIAGNOSTIC'):
            filtered_names[key]=val
    return filtered_names



def write_to_bz2(directory, file_name, data):
    if not os.path.exists(directory):
        os.makedirs(directory)

    with open(directory+file_name, 'a+') as fp:
        for d in data:
            fp.write(str(d[0])+","+str(d[1])+","+str(d[2])+"\n")


if __name__ == '__main__':
    # run with one participant
    # DiagnoseData().one_participant_data(["cd7c2cd6-d0a3-4680-9ba2-0c59d0d0c684"])

    # run for all the participants in a study
    all_participants_data("mperf")