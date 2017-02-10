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

import numpy as np

from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream


def rip_feature_computation(peaks_datastream: DataStream,
                            valleys_datastream: DataStream):
    """
    Respiration Feature Implementation. The respiration feature values are
    derived from the following paper:
    'puffMarker: a multi-sensor approach for pinpointing the timing of first lapse in smoking cessation'


    Removed due to lack of current use in the implementation
    roc_max = []  # 8. ROC_MAX = max(sample[j]-sample[j-1])
    roc_min = []  # 9. ROC_MIN = min(sample[j]-sample[j-1])


    :param peaks_datastream: DataStream
    :param valleys_datastream: DataStream
    :return: RIP Feature DataStreams
    """

    if peaks_datastream is None or valleys_datastream is None:
        return None

    if len(peaks_datastream.data) == 0 or len(valleys_datastream.data) == 0:
        return None

    peaks = np.array([(i.start_time, i.sample) for i in peaks_datastream.data])
    valleys = np.array([(i.start_time, i.sample) for i in valleys_datastream.data])

    # initialize each rip feature array

    insp = []  # 1 Inhalation duration
    expr = []  # 2 Exhalation duration
    resp = []  # 3 Respiration duration
    ieRatio = []  # 4 Inhalation and Exhalation ratio
    stretch = []  # 5 Stretch
    u_stretch = []  # 6. U_Stretch = max(sample[j])
    l_stretch = []  # 7. L_Stretch = min(sample[j])
    bd_insp = []  # 10. BD_INSP = INSP(i)-INSP(i-1)
    bd_expr = []  # 11. BD_EXPR = EXPR(i)-EXPR(i-1)
    bd_resp = []  # 12. BD_RESP = RESP(i)-RESP(i-1)
    bd_stretch = []  # 14. BD_Stretch= Stretch(i)-Stretch(i-1)
    fd_insp = []  # 19. FD_INSP = INSP(i)-INSP(i+1)
    fd_expr = []  # 20. FD_EXPR = EXPR(i)-EXPR(i+1)
    fd_resp = []  # 21. FD_RESP = RESP(i)-RESP(i+1)
    fd_stretch = []  # 23. FD_Stretch= Stretch(i)-Stretch(i+1)
    d5_expr = []  # 29. D5_EXPR(i) = EXPR(i) / avg(EXPR(i-2)...EXPR(i+2))
    d5_stretch = []  # 32. D5_Stretch = Stretch(i) / avg(Stretch(i-2)...Stretch(i+2))

    nCycle = len(valleys) - 1  # Number of respiration cycle

    for i in range(nCycle):
        insp.append(
            DataPoint.from_tuple(start_time=valleys[i][0], sample=(peaks[i][0] - valleys[i][0]).total_seconds()))
        expr.append(
            DataPoint.from_tuple(start_time=valleys[i][0], sample=(valleys[i + 1][0] - peaks[i][0]).total_seconds()))
        resp.append(
            DataPoint.from_tuple(start_time=valleys[i][0], sample=(valleys[i + 1][0] - valleys[i][0]).total_seconds()))

        ier = (peaks[i][0] - valleys[i][0]) / (valleys[i + 1][0] - peaks[i][0])
        ieRatio.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=ier))
        stretch.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=peaks[i][1] - valleys[i + 1][1]))
        u_stretch.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=(peaks[i][1] - valleys[i + 1][1]) / 2))
        l_stretch.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=(-peaks[i][1] + valleys[i + 1][1]) / 2))

    for i in range(nCycle):
        if i == 0:
            bd_insp.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=0))
            bd_expr.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=0))
            bd_resp.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=0))
            bd_stretch.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=0))
        else:
            bd_insp.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=insp[i].sample - insp[i - 1].sample))
            bd_expr.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=expr[i].sample - expr[i - 1].sample))
            bd_resp.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=resp[i].sample - resp[i - 1].sample))
            bd_stretch.append(
                DataPoint.from_tuple(start_time=valleys[i][0], sample=stretch[i].sample - stretch[i - 1].sample))

        if i == nCycle - 1:
            fd_insp.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=0))
            fd_expr.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=0))
            fd_resp.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=0))
            fd_stretch.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=0))
        else:
            fd_insp.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=insp[i].sample - insp[i + 1].sample))
            fd_expr.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=expr[i].sample - expr[i + 1].sample))
            fd_resp.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=resp[i].sample - resp[i + 1].sample))
            fd_stretch.append(
                DataPoint.from_tuple(start_time=valleys[i][0], sample=stretch[i].sample - stretch[i + 1].sample))

        d5Stretch = 0
        d5Exp = 0
        cnt = 0
        for j in [-2, -1, 1, 2]:
            if i + j < 0 or i + j >= nCycle:
                continue
            d5Stretch = d5Stretch + stretch[i + j].sample
            d5Exp = d5Exp + expr[i + j].sample
            cnt = cnt + 1

        d5Stretch = d5Stretch / cnt
        d5Exp = d5Exp / cnt
        d5_stretch.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=stretch[i].sample / d5Stretch))
        d5_expr.append(DataPoint.from_tuple(start_time=valleys[i][0], sample=expr[i].sample / d5Exp))

    insp_datastream = DataStream.from_datastream([peaks_datastream])
    insp_datastream.data = insp

    expr_datastream = DataStream.from_datastream([peaks_datastream])
    expr_datastream.data = expr

    resp_datastream = DataStream.from_datastream([peaks_datastream])
    resp_datastream.data = resp

    ieRatio_datastream = DataStream.from_datastream([peaks_datastream])
    ieRatio_datastream.data = ieRatio

    stretch_datastream = DataStream.from_datastream([peaks_datastream])
    stretch_datastream.data = stretch

    uStretch_datastream = DataStream.from_datastream([peaks_datastream])
    uStretch_datastream.data = u_stretch

    lStretch_datastream = DataStream.from_datastream([peaks_datastream])
    lStretch_datastream.data = l_stretch

    bd_insp_datastream = DataStream.from_datastream([peaks_datastream])
    bd_insp_datastream.data = bd_insp

    bd_expr_datastream = DataStream.from_datastream([peaks_datastream])
    bd_expr_datastream.data = bd_expr

    bd_resp_datastream = DataStream.from_datastream([peaks_datastream])
    bd_resp_datastream.data = bd_resp

    bd_stretch_datastream = DataStream.from_datastream([peaks_datastream])
    bd_stretch_datastream.data = bd_stretch

    fd_insp_datastream = DataStream.from_datastream([peaks_datastream])
    fd_insp_datastream.data = fd_insp

    fd_expr_datastream = DataStream.from_datastream([peaks_datastream])
    fd_expr_datastream.data = fd_expr

    fd_resp_datastream = DataStream.from_datastream([peaks_datastream])
    fd_resp_datastream.data = fd_resp

    fd_stretch_datastream = DataStream.from_datastream([peaks_datastream])
    fd_stretch_datastream.data = fd_stretch

    d5_expr_datastream = DataStream.from_datastream([peaks_datastream])
    d5_expr_datastream.data = d5_expr

    d5_stretch_datastream = DataStream.from_datastream([peaks_datastream])
    d5_stretch_datastream.data = d5_stretch

    return insp_datastream, expr_datastream, resp_datastream, ieRatio_datastream, stretch_datastream, uStretch_datastream, lStretch_datastream, \
           bd_insp_datastream, bd_expr_datastream, bd_resp_datastream, bd_stretch_datastream, fd_insp_datastream, fd_expr_datastream, \
           fd_resp_datastream, fd_stretch_datastream, d5_expr_datastream, d5_stretch_datastream
