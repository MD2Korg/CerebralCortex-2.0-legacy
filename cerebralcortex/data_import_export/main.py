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
import argparse
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_import_export.data_exporter import DataExporter


def run():
    parser = argparse.ArgumentParser(description='CerebralCortex Data Exporter.')
    parser.add_argument("-o", "--output_dir", help="Directory path where exported data will be stored", required=True)
    parser.add_argument("-idz", "--owner_ids", help="Comma separated users' UUIDs", required=False)
    parser.add_argument("-namez", "--owner_user_names", help="Comma separated user-names", required=False)
    parser.add_argument("-nr", "--owner_name_regex", help="User name pattern. For example, '-nr ali' will export all users' data that start with user-name 'ali'", required=False)
    args = vars(parser.parse_args())

    if args["owner_ids"] and (args["owner_user_names"] or args["owner_name_regex"]):
        raise ValueError("Expecting owner_ids: got owner_user_names and/or owner_name_regex too.")
    elif args["owner_user_names"] and (args["owner_ids"] or args["owner_name_regex"]):
        raise ValueError("Expecting owner_user_names: got owner_ids and/or owner_name_regex too.")
    elif args["owner_name_regex"] and (args["owner_ids"] or args["owner_user_names"]):
        raise ValueError("Expecting owner_name_regex: got owner_ids and owner_user_names too.")

    testConfigFile = os.path.join(os.path.dirname(__file__), '../../cerebralcortex.yml')
    CC_obj = CerebralCortex(testConfigFile, master="local[*]", name="Cerebral Cortex Data Importer and Exporter",
                        time_zone="US/Central", load_spark=True)

    if args["owner_ids"]:
        DataExporter(CC_obj, args["output_dir"], owner_ids=args["owner_ids"].split(",")).start()
    elif args["owner_user_names"]:
        DataExporter(CC_obj, args["output_dir"], owner_user_names=args["owner_user_names"].split(",")).start()
    elif args["owner_name_regex"]:
        DataExporter(CC_obj, args["output_dir"], owner_name_regex=args["owner_name_regex"]).start()
    else:
        parser.print_help()
        print("Please provide at least one of these: comma separated owner-ids OR comma separated owner-names OR owner-name pattern")


if __name__=="__main__":
    run()