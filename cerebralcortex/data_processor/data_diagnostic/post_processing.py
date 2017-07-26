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

import json
from collections import OrderedDict

from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.kernel.DataStoreEngine.dataset import DataSet
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.kernel.schema_builder.data_descriptor import data_descriptor
from cerebralcortex.kernel.schema_builder.execution_context import execution_context


def store(input_streams: dict, data: OrderedDict, CC_obj: CerebralCortex, config: dict, algo_type: str):
    """
    Store diagnostic results with its metadata in the data-store
    :param input_streams:
    :param data:
    :param CC_obj:
    :param config:
    :param algo_type:
    """
    parent_stream_id = input_streams[0]["id"]
    stream_name = input_streams[0]["name"]

    result = process_data(stream_name, input_streams, algo_type, config)

    data_descriptor = json.loads(result["dd"])
    execution_context = json.loads(result["ec"])
    annotations = json.loads(result["anno"])

    metadata = CC_obj.get_datastream(parent_stream_id, data_type=DataSet.ONLY_METADATA)

    owner = metadata.owner
    name = metadata.name
    stream_type = "datastream"

    ds = DataStream(owner=owner, name=name, data_descriptor=data_descriptor,
                    execution_context=execution_context, annotations=annotations,
                    stream_type=stream_type, data=data)

    CC_obj.save_datastream(ds)


def process_data(stream_name: str, input_streams: dict, algo_type: str, config: dict) -> dict:
    """
    :param stream_name:
    :param input_streams:
    :param algo_type:
    :param config:
    :return:
    """
    if algo_type == config["algo_names"]["attachment_marker"]:
        result = attachment_marker(stream_name, input_streams, config)
    elif algo_type == config["algo_names"]["battery_marker"]:
        result = battery_data_marker(stream_name, input_streams, config)
    elif algo_type == config["algo_names"]["sensor_unavailable_marker"]:
        result = sensor_unavailable(stream_name, input_streams, config)
    elif algo_type == config["algo_names"]["packet_loss_marker"]:
        result = packet_loss(stream_name, input_streams, config)
    return result


def attachment_marker(stream_name: str, input_streams: dict, config: dict) -> dict:
    """
    :param generated_stream_id:
    :param stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if stream_name == config["sensor_types"]["autosense_ecg"]:
        name = config["output_stream_names"]["ddt_ecg_attachment"]
        input_param = {"window_size": config["general"]["window_size"],
                       "ecg_vairance_threshold": config["attachment_marker"]["ecg_on_body"]}
    elif stream_name == config["sensor_types"]["autosense_rip"]:
        name = config["output_stream_names"]["ddt_rip_attachment"]
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_vairance_threshold": config["attachment_marker"]["rip_on_body"]}
    elif stream_name == config["sensor_types"]["motionsense_accel"]:
        name = config["output_stream_names"]["ddt_motionsense_attachment"]
        input_param = {"window_size": config["general"]["window_size"],
                       "motionsense_vairance_threshold": config["attachment_marker"]["motionsense_on_body"]}
    else:
        raise ValueError("Incorrect sensor type")

    method = 'cerebralcortex.data_processor.data_diagnostic.attachment_marker'
    algo_description = config["description"]["attachment_marker"]

    ec = get_execution_context(name, input_param, input_streams, method,
                               algo_description, config)
    dd = get_data_descriptor(config["algo_names"]["attachment_marker"], config)
    anno = get_annotations()

    return {"ec": ec, "dd": dd, "anno": anno}


def battery_data_marker(stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if stream_name == config["sensor_types"]["phone_battery"]:
        name = config["output_stream_names"]["ddt_phone_battery"]
        input_param = {"window_size": config["general"]["window_size"],
                       "phone_powered_off_threshold": config["battery_marker"]["phone_powered_off"],
                       "phone_battery_down_threshold": config["battery_marker"]["phone_battery_down"]}
    elif stream_name == config["sensor_types"]["autosense_battery"]:
        name = config["output_stream_names"]["ddt_autosense_battery"]
        input_param = {"window_size": config["general"]["window_size"],
                       "autosense_powered_off_threshold": config["battery_marker"]["autosense_powered_off"],
                       "autosense_battery_down_threshold": config["battery_marker"]["autosense_battery_down"]}

    elif stream_name == config["sensor_types"]["motionsense_battery"]:
        name = config["output_stream_names"]["ddt_motionsense_battery"]
        input_param = {"window_size": config["general"]["window_size"],
                       "motionsense_powered_off_threshold": config["battery_marker"]["motionsense_powered_off"],
                       "motionsense_battery_down_threshold": config["battery_marker"]["motionsense_battery_down"]}
    else:
        raise ValueError("Incorrect sensor type")

    algo_description = config["description"]["battery_data_marker"]

    method = 'cerebralcortex.data_processor.data_diagnostic.BatteryDataMarker'

    ec = get_execution_context(name, input_param, input_streams, method, algo_description, config)
    dd = get_data_descriptor(config["algo_names"]["battery_marker"], config)
    anno = get_annotations()
    return {"ec": ec, "dd": dd, "anno": anno}


def sensor_unavailable(sensor_type: str, input_streams: dict, config: dict) -> dict:
    """

    :param sensor_type:
    :param input_streams:
    :param config:
    :return:
    """
    if sensor_type == config["sensor_types"]["autosense_ecg"]:
        name = config["output_stream_names"]["ddt_autosense_unavailable"]
        input_param = {"window_size": config["general"]["window_size"],
                       "sensor_unavailable_threshold": config["sensor_unavailable_marker"]["ecg"]}
    elif sensor_type == config["sensor_types"]["autosense_rip"]:
        name = config["output_stream_names"]["ddt_autosense_unavailable"]
        input_param = {"window_size": config["general"]["window_size"],
                       "sensor_unavailable_threshold": config["sensor_unavailable_marker"]["rip"]}

    elif sensor_type == config["sensor_types"]["motionsense_accel"]:
        name = config["output_stream_names"]["ddt_motionsense_unavailable"]
        input_param = {"window_size": config["general"]["window_size"],
                       "sensor_unavailable_threshold": config["sensor_unavailable_marker"]["motionsense"]}
    else:
        raise ValueError("Incorrect sensor type")

    algo_description = config["description"]["sensor_unavailable_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.packet_loss_marker'

    ec = get_execution_context(name, input_param, input_streams, method,
                               algo_description, config)
    dd = get_data_descriptor(config["algo_names"]["sensor_unavailable_marker"], config)
    anno = get_annotations()
    return {"ec": ec, "dd": dd, "anno": anno}


def packet_loss(sensor_type: str, input_streams: dict, config: dict) -> dict:
    """

    :param sensor_type:
    :param input_streams:
    :param config:
    :return:
    """
    if sensor_type == config["sensor_types"]["autosense_ecg"]:
        name = config["output_stream_names"]["ddt_ecg_packet_loss"]
        input_param = {"window_size": config["general"]["window_size"],
                       "ecg_acceptable_packet_loss": config["packet_loss_marker"]["ecg_acceptable_packet_loss"]}
    elif sensor_type == config["sensor_types"]["autosense_rip"]:
        name = config["output_stream_names"]["ddt_rip_packet_loss"]
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_acceptable_packet_loss": config["packet_loss_marker"]["rip_acceptable_packet_loss"]}

    elif sensor_type == config["sensor_types"]["motionsense_accel"]:
        name = config["output_stream_names"]["ddt_motionsense_packet_loss"]
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_acceptable_packet_loss": config["packet_loss_marker"]["motionsense_acceptable_packet_loss"]}
    else:
        raise ValueError("Incorrect sensor type")

    algo_description = config["description"]["packet_loss_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.packet_loss_marker'

    ec = get_execution_context(name, input_param, input_streams, method,
                               algo_description, config)
    dd = get_data_descriptor(config["algo_names"]["packet_loss_marker"], config)
    anno = get_annotations()
    return {"ec": ec, "dd": dd, "anno": anno}


# TO-DO: Only return data descriptor for one sensor
def get_data_descriptor(algo_type: str, config: dict) -> dict:
    """
    :param algo_type:
    :param config:
    :return:
    """
    if algo_type == config["algo_names"]["battery_marker"]:
        dd = {"phone_powered_off": config["labels"]["phone_powered_off"],
              "phone_battery_down": config["labels"]["phone_battery_down"],
              "autosesen_powered_off": config["labels"]["autosesen_powered_off"],
              "autosense_battery_down": config["labels"]["autosense_battery_down"],
              "motionsense_powered_off": config["labels"]["motionsense_powered_off"],
              "motionsense_battery_down": config["labels"]["motionsense_battery_down"]}
    elif algo_type == config["algo_names"]["attachment_marker"]:
        dd = {"ecg_improper_attachment": config["labels"]["ecg_improper_attachment"],
              "ecg_off_body": config["labels"]["ecg_off_body"],
              "ecg_on_body": config["labels"]["ecg_on_body"],
              "rip_improper_attachment": config["labels"]["rip_improper_attachment"],
              "rip_off_body": config["labels"]["rip_off_body"],
              "rip_on_body": config["labels"]["rip_on_body"],
              "motionsense_improper_attachment": config["labels"]["motionsense_improper_attachment"],
              "motionsense_off_body": config["labels"]["motionsense_off_body"],
              "motionsense__on_body": config["labels"]["motionsense_on_body"]}
    elif algo_type == config["algo_names"]["sensor_unavailable_marker"]:
        dd = {"autosense_unavailable": config["labels"]["autosense_unavailable"],
              "motionsense_unavailable": config["labels"]["motionsense_unavailable"]}
    elif algo_type == config["algo_names"]["packet_loss_marker"]:
        dd = {"ecg_packet_loss": config["labels"]["ecg_packet_loss"],
              "rip_packet_loss": config["labels"]["rip_packet_loss"],
              "motionsense_packet_loss": config["labels"]["motionsense_packet_loss"]}

    return json.dumps(data_descriptor.get_data_descriptor("label", "window", dd))


def get_execution_context(name: str, input_param: dict, input_streams: dict, method: str,
                          algo_description: str, config: dict) -> dict:
    """
    :param name:
    :param input_param:
    :param input_streams:
    :param method:
    :param algo_description:
    :param config:
    :return:
    """

    author = [{"name": "Ali", "email": "nasir.ali08@gmail.com"}]
    version = '0.0.1'
    ref = {"url": "http://www.cs.memphis.edu/~santosh/Papers/Continuous-Stress-BCB-2014.pdf"}

    processing_module = execution_context().processing_module_schema(name, config["description"]["data_diagnostic"],
                                                                     input_param, input_streams)
    algorithm = execution_context().algorithm_schema(method, algo_description, author, version, ref)

    ec = execution_context().get_execution_context(processing_module, algorithm)
    return ec


def get_annotations() -> dict:
    """
    :return:
    """
    annotations = []
    return json.dumps(annotations)
