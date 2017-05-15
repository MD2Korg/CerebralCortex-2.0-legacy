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


from cerebralcortex.kernel.schema_builder.execution_context import execution_context


def get_execution_context(pm_algo_name: str, old_schema: dict) -> dict:
    """
    :param pm_algo_name:
    :param old_schema:
    :return:
    """

    old_schema = {"old_schema": old_schema}

    processing_module = execution_context().processing_module_schema(pm_algo_name, "blank", "blank", "blank")
    algorithm = execution_context().algorithm_schema(pm_algo_name, "blank", "blank", "blank", "blank")

    algo_and_old_schema = {**algorithm, **old_schema}

    ec = execution_context().get_execution_context(processing_module, algo_and_old_schema)
    return ec


def get_data_descriptor(old_schema: dict) -> dict:
    """

    :param old_schema:
    :return:
    """
    return old_schema["datadescriptor"]


def get_annotations() -> dict:
    """

    :return:
    """
    return []
