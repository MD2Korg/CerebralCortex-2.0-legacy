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
from cerebralcortex.data_imp_exp.data_exporter import DataExporter


def run():
    testConfigFile = os.path.join(os.path.dirname(__file__), '../../cerebralcortex.yml')
    CC_obj = CerebralCortex(testConfigFile, master="local[*]", name="Cerebral Cortex DataStoreEngine Tests",
                        time_zone="US/Central", load_spark=True)

    output_dir = "/home/ali/Desktop/DUMP/aaaa/"

    de = DataExporter(CC_obj, output_dir, owner_ids="de5b4a7d-ba1b-44c4-b55e-cd0ca7487734")
    de.start()


if __name__=="__main__":
    run()