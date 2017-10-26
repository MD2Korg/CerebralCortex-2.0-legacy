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


import logging
import inspect
import os
logs_dir = '/var/log/cerebralcortex/'


def cc_log(error_message="", error_type="EXCEPTION"):

    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    error_type = error_type.upper()
    FORMAT = '[%(asctime)s] - %(message)s'

    execution_stats = inspect.stack()
    method_name = execution_stats[1][3]
    file_name = execution_stats[1][1]
    line_number = execution_stats[1][2]

    error_message = "[" + str(file_name) + " - " + str(method_name) + " - " + str(line_number) + "] - " + str(error_message)

    if error_type=="CRITICAL":
        logs_filename = logs_dir+"critical.log"
        logging.basicConfig(filename=logs_filename,level=logging.CRITICAL, format=FORMAT)
        logging.critical(error_message)
    elif error_type == "ERROR":
        logs_filename = logs_dir+"error.log"
        logging.basicConfig(filename=logs_filename,level=logging.ERROR, format=FORMAT)
        logging.error(error_message)
    elif error_type == "EXCEPTION":
        logs_filename = logs_dir+"error.log"
        logging.basicConfig(filename=logs_filename,level=logging.ERROR, format=FORMAT)
        logging.exception(error_message)
    elif error_type == "WARNING":
        logs_filename = logs_dir+"warning.log"
        logging.basicConfig(filename=logs_filename,level=logging.WARNING, format=FORMAT)
        logging.warning(error_message)
    elif error_type == "DEBUG":
        logs_filename = logs_dir+"debug.log"
        logging.basicConfig(filename=logs_filename,level=logging.DEBUG, format=FORMAT)
        logging.debug(error_message)
    elif error_type == "MISSING_DATA":
        logs_filename = logs_dir+"missing_data.log"
        logging.basicConfig(filename=logs_filename,level=logging.WARNING, format=FORMAT)
        logging.warning(error_message)
    else:
        logs_filename = logs_dir+"info.log"
        logging.basicConfig(filename=logs_filename,level=logging.INFO, format=FORMAT)
        logging.info(error_message)

