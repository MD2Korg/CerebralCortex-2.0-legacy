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
import uuid
from datetime import timedelta
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.motionsense_hrv_led_quality.data_quality_led import data_quality_led
from cerebralcortex.data_processor.motionsense_hrv_led_quality.post_processing import store
from cerebralcortex.data_processor.signalprocessing.window import window
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet

# create and load CerebralCortex object and configs
configuration_file = os.path.join(os.path.dirname(__file__), '../../../cerebralcortex.yml')
CC_driver = CerebralCortex(configuration_file, master="local[*]", name="LED Data Quality", load_spark=True)
CC_worker = CerebralCortex(configuration_file, master="local[*]", name="LED Data Quality", load_spark=False)


def all_participants_data(study_name):
    # get all participants' name-ids
    participants = CC_driver.get_all_participants(study_name)

    if len(participants) > 0:
        participants_rdd = CC_driver.sc.parallelize(participants)
        results = participants_rdd.map(
            lambda participant: process_data(participant["identifier"], CC_worker))
        results.count()
    else:
        print("No participant is selected")


def process_data(owner_id, CC):
    streams = CC.get_participant_streams(owner_id)
    if streams and len(streams) > 0:
        led_right_wrist_quality_stream_name = "DATA_QUALITY--LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"
        led_left_wrist_quality_stream_name = "DATA_QUALITY--LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"

        analyze_quality(streams, owner_id, led_right_wrist_quality_stream_name, "right", CC)
        analyze_quality(streams, owner_id, led_left_wrist_quality_stream_name, "left", CC)


def analyze_quality(streams, owner_id, led_right_wrist_quality_stream_name, wrist, CC):
    led_stream_quality_id = uuid.uuid3(uuid.NAMESPACE_DNS, str(led_right_wrist_quality_stream_name + owner_id+"LED quality computed on CerebralCortex"))
    if wrist=="right":
        if "LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST" in streams:
            led_wrist_stream_id = streams["LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"][
                "identifier"]
            led_wrist_stream_name = streams["LED--org.md2k.motionsense--MOTION_SENSE_HRV--RIGHT_WRIST"]["name"]
        else:
            led_wrist_stream_id = None
    else:
        if "LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST" in streams:
            led_wrist_stream_id = streams["LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"][
                "identifier"]
            led_wrist_stream_name = streams["LED--org.md2k.motionsense--MOTION_SENSE_HRV--LEFT_WRIST"]["name"]
        else:
            led_wrist_stream_id = None

    if led_wrist_stream_id:
        stream_end_days = CC.get_stream_start_end_time(led_wrist_stream_id)
        if stream_end_days["start_time"] and stream_end_days["end_time"]:
            days = stream_end_days["end_time"] - stream_end_days["start_time"]
            for day in range(days.days + 1):
                day = (stream_end_days["start_time"]+timedelta(days=day)).strftime('%Y%m%d')
                stream = CC.get_datastream(led_wrist_stream_id, data_type=DataSet.COMPLETE, day=day)
                if len(stream.data) > 0:
                    windowed_data = window(stream.data, 3, False)
                    led_quality_windows = data_quality_led(windowed_data)

                    input_streams = [{"owner_id": str(owner_id), "id": str(led_wrist_stream_id),
                                      "name": led_wrist_stream_name}]
                    output_stream = {"id": str(led_stream_quality_id), "name": led_right_wrist_quality_stream_name, "algo_type": ""}

                    store(led_quality_windows, input_streams, output_stream, CC)

if __name__ == '__main__':
    # run for all the participants in a study
    all_participants_data("mperf")
