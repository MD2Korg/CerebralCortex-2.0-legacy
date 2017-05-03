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


class execution_context:

    def get_execution_context(self, processing_module: dict, algorithm: dict) -> dict:
        """
        Please have a look at /kernel/schema/examples/ for schema and field details
        :param processing_module:
        :param algorithm:
        """
        processing_module = self.processing_module_schema(processing_module)
        algorithm = self.algorithm_schema(algorithm)

        ec = {**processing_module, **algorithm}
        execution_context = {"execution_context": ec}
        return execution_context

    @staticmethod
    def processing_module_schema(pm: dict):
        """

        :param pm:
        :return:
        """
        if not pm:
            raise ValueError("Processing module schema cannot be empty.")
        elif not pm["name"]:
            raise ValueError("Processing module name cannot be empty.")

        elif not pm["description"]:
            raise ValueError("Processing module description cannot be empty.")

        elif not pm["output_streams"]:
            raise ValueError("Name of output stream cannot be empty.")
        else:
            return {"processing_module": pm}

    @staticmethod
    def algorithm_schema(algo: dict):
        """

        :param algo:
        :return:
        """
        if not algo:
            raise ValueError("Algorithm schema cannot be empty.")
        elif not algo["method"]:
            raise ValueError("Algorithm package path cannot be empty.")
        elif not algo["description"]:
            raise ValueError("Algorithm description cannot be empty.")
        elif not algo["authors"]:
            raise ValueError("Algorithm's author/developer name cannot be empty.")
        elif not algo["version"]:
            raise ValueError("Algorithm's version number cannot be empty.")
        else:
            return {"algorithm": algo}