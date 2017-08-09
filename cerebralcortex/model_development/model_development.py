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

from datetime import datetime,timedelta
import pytz


import numpy as np
from collections import Counter
from pprint import pprint
from sklearn import svm, metrics, preprocessing
from sklearn.cross_validation import LabelKFold
from cerebralcortex.model_development.modifiedGridSearchCV import ModifiedGridSearchCV, cross_val_probs
from cerebralcortex.model_development.modifiedRandomizedSearchCV import ModifiedRandomizedSearchCV
from cerebralcortex.model_development.saveModel import saveModel




def decode_label(label):
    label = label[:2]  # Only the first 2 characters designate the label code
    mapping = {'c1': 0, 'c2': 1, 'c3': 1, 'c4': 0, 'c5': 0, 'c6': 0, 'c7': 2, }
    return mapping[label]


def check_stress_mark(stress_mark, start_time):
    endtime = start_time +  timedelta(seconds=60) # One minute windows
    result = []
    for dp in stress_mark:
        st = dp.start_time
        et = dp.end_time
        gt = dp.sample[0][:2]
        if gt not in ['c7']:
            if (start_time > st) and (endtime < et):
                result.append(gt)
    data = Counter(result)
    return data.most_common(1)


def analyze_events_with_features(participant,stress_mark_stream,feature_stream):
    stress_marks = stress_mark_stream.data

    start_times = {}

    for dp in stress_marks:
        if dp.sample[0][:2] == 'c4':
            if participant not in start_times:
                start_times[participant] = datetime.utcnow().replace(tzinfo=pytz.UTC)
            if dp.start_time < start_times[participant]:
                start_times[participant] = dp.start_time
    feature_matrix = feature_stream.data

    feature_labels = []
    final_features = []
    subjects = []


    for feature_vector in feature_matrix:
        ts = feature_vector.start_time
        f = feature_vector.sample

        if ts < start_times[participant]:
            continue  # Outside of starting time

        label = check_stress_mark(stress_marks, ts)

        if len(label) > 0:
            stress_class = decode_label(label[0][0])

            feature_labels.append(stress_class)
            final_features.append(f)
            subjects.append(participant)

    return final_features, feature_labels, subjects


def feature_label_separator(features:list):
    """

    :param features: Nested List containing three individual lists = feature vectors, labels and Subject ID
    :return: separated lists
    """
    traindata = []
    trainlabels = []
    subjects = []
    for feature_list in features:
        traindata.extend(feature_list[0])
        trainlabels.extend(feature_list[1])
        subjects.extend(feature_list[2])

    return traindata, trainlabels, subjects

def Twobias_scorer_CV(probs, y, ret_bias=False):
    db = np.transpose(np.vstack([probs, y]))
    db = db[np.argsort(db[:, 0]), :]

    pos = np.sum(y == 1)
    n = len(y)
    neg = n - pos
    tp, tn = pos, 0
    lost = 0

    optbias = []
    minloss = 1

    for i in range(n):
        #		p = db[i,1]
        if db[i, 1] == 1:  # positive
            tp -= 1.0
        else:
            tn += 1.0

        # v1 = tp/pos
        #		v2 = tn/neg
        if tp / pos >= 0.95 and tn / neg >= 0.95:
            optbias = [db[i, 0], db[i, 0]]
            continue

        running_pos = pos
        running_neg = neg
        running_tp = tp
        running_tn = tn

        for j in range(i + 1, n):
            #			p1 = db[j,1]
            if db[j, 1] == 1:  # positive
                running_tp -= 1.0
                running_pos -= 1
            else:
                running_neg -= 1

            lost = (j - i) * 1.0 / n
            if running_pos == 0 or running_neg == 0:
                break

            # v1 = running_tp/running_pos
            #			v2 = running_tn/running_neg

            if running_tp / running_pos >= 0.95 and running_tn / running_neg >= 0.95 and lost < minloss:
                minloss = lost
                optbias = [db[i, 0], db[j, 0]]

    if ret_bias:
        return -minloss, optbias
    else:
        return -minloss


def f1Bias_scorer_CV(probs, y, ret_bias=False):
    precision, recall, thresholds = metrics.precision_recall_curve(y, probs)

    f1 = 0.0
    for i in range(0, len(thresholds)):
        if not (precision[i] == 0 and recall[i] == 0):
            f = 2 * (precision[i] * recall[i]) / (precision[i] + recall[i])
            if f > f1:
                f1 = f
                bias = thresholds[i]

    if ret_bias:
        return f1, bias
    else:
        return f1



def cstress_model(features:list,
                  n_iter:int=20,
                  scorer: str='f1',
                  searchtype: str='grid',
                  outputfilename:str='output.txt'):
    """

    :param features: Nested List containing three individual lists = feature vectors, labels and Subject ID
    :param scorer:
    :return: returns the cStress Model
    """
    traindata, trainlabels, subjects = feature_label_separator(features)


    traindata = np.asarray(traindata, dtype=np.float64)
    trainlabels = np.asarray(trainlabels)

    normalizer = preprocessing.StandardScaler()
    traindata = normalizer.fit_transform(traindata)

    lkf = LabelKFold(subjects, n_folds=len(np.unique(subjects)))

    delta = 0.1
    parameters = {'kernel': ['rbf'],
                  'C': [2 ** x for x in np.arange(-12, 12, 0.5)],
                  'gamma': [2 ** x for x in np.arange(-12, 12, 0.5)],
                  'class_weight': [{0: w, 1: 1 - w} for w in np.arange(0.0, 1.0, delta)]}

    svc = svm.SVC(probability=True, verbose=False, cache_size=2000)

    if scorer == 'f1':
        scorer_func = f1Bias_scorer_CV
    else:
        scorer_func = Twobias_scorer_CV

    if searchtype == 'grid':
        clf = ModifiedGridSearchCV(svc, parameters, cv=lkf, n_jobs=-1, scoring=scorer_func, verbose=1, iid=False)
    else:
        clf = ModifiedRandomizedSearchCV(estimator=svc, param_distributions=parameters, cv=lkf, n_jobs=-1,
                                         scoring=scorer_func, n_iter=n_iter,
                                         verbose=1, iid=False)

    clf.fit(traindata, trainlabels)
    pprint(clf.best_params_)

    CV_probs = cross_val_probs(clf.best_estimator_, traindata, trainlabels, lkf)
    score, bias = scorer_func(CV_probs, trainlabels, True)
    print(score, bias)
    if not bias == []:
        saveModel(outputfilename, clf.best_estimator_, normalizer, bias)

        n = len(trainlabels)

        if scorer == 'f1':
            predicted = np.asarray(CV_probs >= bias, dtype=np.int)
            classified = range(n)
        else:
            classified = np.where(np.logical_or(CV_probs <= bias[0], CV_probs >= bias[1]))[0]
            predicted = np.asarray(CV_probs[classified] >= bias[1], dtype=np.int)

        print("Cross-Subject (" + str(len(np.unique(subjects))) + "-fold) Validation Prediction")
        print("Accuracy: " + str(metrics.accuracy_score(trainlabels[classified], predicted)))
        print(metrics.classification_report(trainlabels[classified], predicted))
        print(metrics.confusion_matrix(trainlabels[classified], predicted))
        print("Lost: %d (%f%%)" % (n - len(classified), (n - len(classified)) * 1.0 / n))
        print("Subjects: " + str(np.unique(subjects)))
    else:
        print ("Results not good")




