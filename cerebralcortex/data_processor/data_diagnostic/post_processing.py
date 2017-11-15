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


def store(data: OrderedDict, input_streams: dict, output_streams: dict, CC_obj: CerebralCortex, config:dict):
    """
    Store diagnostic results with its metadata in the data-store
    :param input_streams:
    :param data:
    :param CC_obj:
    :param config:
    :param algo_type:
    """
    if data:
        #basic output stream info
        owner = input_streams[0]["owner_id"]
        dd_stream_id = output_streams["id"]
        dd_stream_name = output_streams["name"]
        stream_type = "ds"

        result = process_data(dd_stream_name, input_streams, output_streams["algo_type"], config)

        data_descriptor = result["dd"]
        execution_context = result["ec"]
        annotations = result["anno"]

        ds = DataStream(identifier=dd_stream_id, owner=owner, name=dd_stream_name, data_descriptor=data_descriptor,
                        execution_context=execution_context, annotations=annotations,
                        stream_type=stream_type, data=data)

        CC_obj.save_datastream(ds,"datastream")


def process_data(dd_stream_name: str, input_streams: dict, algo_type: str, config: dict) -> dict:
    """
    :param dd_stream_name:
    :param input_streams:
    :param algo_type:
    :param config:
    :return:
    """
    if algo_type == config["algo_type"]["attachment_marker"]:
        result = attachment_marker(dd_stream_name, input_streams, config)
    elif algo_type == config["algo_type"]["battery_marker"]:
        result = battery_data_marker(dd_stream_name, input_streams, config)
    elif algo_type == config["algo_type"]["sensor_unavailable_marker"]:
        result = sensor_unavailable(dd_stream_name, input_streams, config)
    elif algo_type == config["algo_type"]["packet_loss_marker"]:
        result = packet_loss(dd_stream_name, input_streams, config)
    elif algo_type == config["algo_type"]["app_availability_marker"]:
        result = app_unavailable(dd_stream_name, input_streams, config)
    elif algo_type == config["algo_type"]["sensor_failure"]:
        result = sensor_failure(dd_stream_name, input_streams, config)
    elif algo_type == config["algo_type"]["phone_screen_touch"]:
        result = phone_screen_touch(dd_stream_name, input_streams, config)
    return result


def attachment_marker(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """
    :param generated_stream_id:
    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if dd_stream_name == config["stream_names"]["autosense_rip_attachment_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "onbody_threshold": config["attachment_marker"]["rip_on_body"],
                       "improper_attachment":config["attachment_marker"]["improper_attachment"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Attachment labels: Improper attachment: "+ str(config["labels"]["rip_improper_attachment"])+", Offbody: "+str(config["labels"]["rip_off_body"])+", Onbody: "+str(config["labels"]["rip_on_body"])}
    elif dd_stream_name == config["stream_names"]["autosense_ecg_attachment_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "ecg_vairance_threshold": config["attachment_marker"]["ecg_on_body"],
                       "improper_attachment":config["attachment_marker"]["improper_attachment"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Attachment labels: Improper attachment: "+ str(config["labels"]["ecg_improper_attachment"])+", Offbody: "+str(config["labels"]["ecg_off_body"])+", Onbody: "+str(config["labels"]["ecg_on_body"])}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_right_attachment_marker"] or dd_stream_name == config["stream_names"]["motionsense_hrv_left_attachment_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "motionsense_improper_attachment_threshold": config["attachment_marker"]["motionsense_improper_attachment"],
                       "motionsense_onbody_threshold": config["attachment_marker"]["motionsense_onbody"],
                       "motionsense_offbody_threshold": config["attachment_marker"]["motionsense_offbody"]
                       }
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Attachment labels: Improper attachment: "+ str(config["labels"]["motionsense_improper_attachment"])+", Offbody: "+str(config["labels"]["motionsense_offbody"])+", Onbody: "+str(config["labels"]["motionsense_onbody"])}
    else:
        raise ValueError("Incorrect sensor type")

    method = 'cerebralcortex.data_processor.data_diagnostic.attachment_marker'
    algo_description = config["description"]["attachment_marker"]
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}


def battery_data_marker(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if dd_stream_name == config["stream_names"]["phone_battery_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "phone_powered_off_threshold": config["battery_marker"]["phone_powered_off"],
                       "phone_battery_down_threshold": config["battery_marker"]["phone_battery_down"]}
    elif dd_stream_name == config["stream_names"]["autosense_battery_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "autosense_powered_off_threshold": config["battery_marker"]["autosense_powered_off"],
                       "autosense_battery_down_threshold": config["battery_marker"]["autosense_battery_down"]}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_battery_right_marker"] or dd_stream_name == config["stream_names"]["motionsense_hrv_battery_left_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "motionsense_powered_off_threshold": config["battery_marker"]["motionsense_powered_off"],
                       "motionsense_battery_down_threshold": config["battery_marker"]["motionsense_battery_down"]}
    else:
        raise ValueError("Incorrect sensor type")

    data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Labels - Powered off"+ str(config["labels"]["powered_off"])+", Battery down"+ str(config["labels"]["battery_down"])}
    algo_description = config["description"]["battery_data_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.battery_data_marker.py'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method, algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}


def sensor_unavailable(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if dd_stream_name == config["stream_names"]["autosense_wireless_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "sensor_unavailable_ecg_threshold": config["sensor_unavailable_marker"]["ecg"],
                       "sensor_unavailable_rip_threshold": config["sensor_unavailable_marker"]["rip"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "AutoSense unavailable label: "+ str(config["labels"]["autosense_unavailable"])}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_right_wireless_marker"] or dd_stream_name == config["stream_names"]["motionsense_hrv_left_wireless_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "sensor_unavailable_motionsense_threshold": config["sensor_unavailable_marker"]["motionsense"],
                       "sensor_unavailable_phone_threshold": config["sensor_unavailable_marker"]["phone"]
                       }
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Motionsense unavailable label: "+ str(config["labels"]["motionsense_unavailable"])}
    else:
        raise ValueError("Incorrect sensor type")

    algo_description = config["description"]["sensor_unavailable_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.sensor_unavailable_marker'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}

def app_unavailable(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    input_param = {"window_size": config["general"]["window_size"],
                       "app_availability_marker_battery_threshold": "1"}
    data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "mobile phone availability: "+ str(config["labels"]["app_unavailable"])+", "+ str(config["labels"]["app_available"])}

    algo_description = config["description"]["app_availability_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.app_availability.py'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}

def phone_screen_touch(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    input_param = {"window_size": config["general"]["window_size"]}
    data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Participant's active and inactive periods on phone: labels: touch, no-touch"}

    algo_description = config["description"]["phone_screen_touch"]
    method = 'cerebralcortex.data_processor.data_diagnostic.util.phone_screen_touch.py'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}

def sensor_failure(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    input_param = {"window_size": "21600"}
    if dd_stream_name==config["stream_names"]["motionsense_hrv_right_sensor_failure_marker"] or dd_stream_name==config["stream_names"]["motionsense_hrv_left_sensor_failure_marker"]:
        label = config["labels"]["motionsense_failure"]
    elif dd_stream_name==dd_stream_name==config["stream_names"]["phone_sensor_failure_marker"]:
        label = config["labels"]["phone_sensor_failure"]
    elif dd_stream_name==dd_stream_name==config["stream_names"]["autosense_sensor_failure_marker"]:
        label = config["labels"]["autosense_sensor_failure"]
    else:
        raise ValueError("Incorrect sensor type")

    data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "sensor failure detection: "+ str(label)}
    algo_description = config["description"]["sensor_failure"]
    method = 'cerebralcortex.data_processor.data_diagnostic.sensor_failure'
    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}


def packet_loss(dd_stream_name: str, input_streams: dict, config: dict) -> dict:
    """

    :param dd_stream_name:
    :param input_streams:
    :param config:
    :return:
    """
    if dd_stream_name == config["stream_names"]["autosense_ecg_packetloss_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "ecg_acceptable_packet_loss": config["packet_loss_marker"]["ecg_acceptable_packet_loss"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Packet-Loss label: "+ str(config["labels"]["ecg_packet_loss"])}
    elif dd_stream_name == config["stream_names"]["autosense_rip_packetloss_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_acceptable_packet_loss": config["packet_loss_marker"]["rip_acceptable_packet_loss"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Packet-Loss label: "+ str(config["labels"]["rip_packet_loss"])}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_accel_right_packetloss_marker"] or dd_stream_name == config["stream_names"]["motionsense_hrv_accel_left_packetloss_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_acceptable_packet_loss": config["packet_loss_marker"]["motionsense_accel_acceptable_packet_loss"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Packet-Loss label: "+ str(config["labels"]["motionsense_accel_packet_loss"])}
    elif dd_stream_name == config["stream_names"]["motionsense_hrv_gyro_right_packetloss_marker"] or dd_stream_name == config["stream_names"]["motionsense_hrv_gyro_left_packetloss_marker"]:
        input_param = {"window_size": config["general"]["window_size"],
                       "rip_acceptable_packet_loss": config["packet_loss_marker"]["motionsense_gyro_acceptable_packet_loss"]}
        data_descriptor = {"NAME": dd_stream_name, "DATA_TYPE": "int", "DESCRIPTION": "Packet-Loss label: "+ str(config["labels"]["motionsense_gyro_packet_loss"])}
    else:
        raise ValueError("Incorrect sensor type")

    algo_description = config["description"]["packet_loss_marker"]
    method = 'cerebralcortex.data_processor.data_diagnostic.packet_loss_marker.py'

    ec = get_execution_context(dd_stream_name, input_param, input_streams, method,
                               algo_description, config)
    anno = get_annotations()
    return {"ec": ec, "dd": data_descriptor, "anno": anno}


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
    return annotations
