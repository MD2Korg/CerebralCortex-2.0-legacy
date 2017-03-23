# Copyright (c) 2015, University of Memphis, MD2K Center of Excellence
#  - Timothy Hnat <twhnat@memphis.edu>
#  - Karen Hovsepian <karoaper@gmail.com>
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

import argparse
import json
import numpy as np
from collections import Counter
from collections import Sized
from pathlib import Path
from pprint import pprint
from sklearn import svm, metrics, preprocessing
from sklearn.base import clone, is_classifier
from sklearn.cross_validation import LabelKFold
from sklearn.model_selection import check_cv
from sklearn.externals.joblib import Parallel, delayed
from sklearn.model_selection import GridSearchCV, RandomizedSearchCV, ParameterSampler, ParameterGrid
from sklearn.utils.validation import _num_samples, indexable

#new import
import gzip
import os
import time
import uuid
import pandas as pd
from pprint import pprint
from datetime import datetime
import pytz


from pyspark import RDD
from cerebralcortex.CerebralCortex import CerebralCortex
from cerebralcortex.data_processor.cStress import cStress
from cerebralcortex.data_processor.preprocessor import parser
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import DataStream
from cerebralcortex.legacy import find



argparser = argparse.ArgumentParser(description="Cerebral Cortex cStress Test Application")
argparser.add_argument('--base_directory')
args = argparser.parse_args()

# To run this program, please specific a program argument for base_directory that is the path to the test data files.
# e.g. --base_directory /Users/hnat/data/
basedir = args.base_directory

configuration_file = os.path.join(os.path.dirname(__file__), '/home/anjana/IdeaProjects/CerebralCortexcstessmodel/cerebralcortex.yml')

CC = CerebralCortex(configuration_file, master="local[*]", name="Memphis cStress Development App")


def cv_fit_and_score(estimator, X, y, scorer, parameters, cv, ):
    """Fit estimator and compute scores for a given dataset split.
    Parameters
    ----------
    estimator : estimator object implementing 'fit'
        The object to use to fit the data.
    X : array-like of shape at least 2D
        The data to fit.
    y : array-like, optional, default: None
        The target variable to try to predict in the case of
        supervised learning.
    scorer : callable
        A scorer callable object / function with signature
        ``scorer(estimator, X, y)``.
    parameters : dict or None
        Parameters to be set on the estimator.
    cv:	Cross-validation fold indeces
    Returns
    -------
    score : float
        CV score on whole set.
    parameters : dict or None, optional
        The parameters that have been evaluated.
    """
    estimator.set_params(**parameters)
    cv_probs_ = cross_val_probs(estimator, X, y, cv)
    score = scorer(cv_probs_, y)

    return [score, parameters]  # scoring_time]


class ModifiedGridSearchCV(GridSearchCV):
    def __init__(self, estimator, param_grid, scoring=None, fit_params=None,
                 n_jobs=1, iid=True, refit=True, cv=None, verbose=0,
                 pre_dispatch='2*n_jobs', error_score='raise'):

        super(ModifiedGridSearchCV, self).__init__(
            estimator, param_grid, scoring, fit_params, n_jobs, iid,
            refit, cv, verbose, pre_dispatch, error_score)

    def fit(self, X, y):
        """Actual fitting,  performing the search over parameters."""

        parameter_iterable = ParameterGrid(self.param_grid)

        estimator = self.estimator
        cv = self.cv

        n_samples = _num_samples(X)
        X, y = indexable(X, y)

        if y is not None:
            if len(y) != n_samples:
                raise ValueError('Target variable (y) has a different number '
                                 'of samples (%i) than data (X: %i samples)'
                                 % (len(y), n_samples))
        cv = check_cv(cv, X, y, classifier=is_classifier(estimator))

        if self.verbose > 0:
            if isinstance(parameter_iterable, Sized):
                n_candidates = len(parameter_iterable)
                print("Fitting {0} folds for each of {1} candidates, totalling"
                      " {2} fits".format(len(cv), n_candidates,
                                         n_candidates * len(cv)))

        base_estimator = clone(self.estimator)

        pre_dispatch = self.pre_dispatch

        out = Parallel(
            n_jobs=self.n_jobs, verbose=self.verbose,
            pre_dispatch=pre_dispatch
        )(
            delayed(cv_fit_and_score)(clone(base_estimator), X, y, self.scoring,
                                      parameters, cv=cv)
            for parameters in parameter_iterable)

        best = sorted(out, reverse=True)[0]
        self.best_params_ = best[1]
        self.best_score_ = best[0]

        if self.refit:
            # fit the best estimator using the entire dataset
            # clone first to work around broken estimators
            best_estimator = clone(base_estimator).set_params(
                **best[1])
            if y is not None:
                best_estimator.fit(X, y, **self.fit_params)
            else:
                best_estimator.fit(X, **self.fit_params)
            self.best_estimator_ = best_estimator

        return self


class ModifiedRandomizedSearchCV(RandomizedSearchCV):
    def __init__(self, estimator, param_distributions, n_iter=10, scoring=None,
                 fit_params=None, n_jobs=1, iid=True, refit=True, cv=None,
                 verbose=0, pre_dispatch='2*n_jobs', random_state=None,
                 error_score='raise'):

        super(ModifiedRandomizedSearchCV, self).__init__(estimator=estimator, param_distributions=param_distributions,
                                                         n_iter=n_iter, scoring=scoring, random_state=random_state,
                                                         fit_params=fit_params, n_jobs=n_jobs, iid=iid, refit=refit,
                                                         cv=cv, verbose=verbose, pre_dispatch=pre_dispatch,
                                                         error_score=error_score)

    def fit(self, X, y):
        """Actual fitting,  performing the search over parameters."""

        parameter_iterable = ParameterSampler(self.param_distributions,
                                              self.n_iter,
                                              random_state=self.random_state)
        estimator = self.estimator
        cv = self.cv

        n_samples = _num_samples(X)
        X, y = indexable(X, y)

        if y is not None:
            if len(y) != n_samples:
                raise ValueError('Target variable (y) has a different number '
                                 'of samples (%i) than data (X: %i samples)'
                                 % (len(y), n_samples))
        cv = check_cv(cv, X, y, classifier=is_classifier(estimator))

        if self.verbose > 0:
            if isinstance(parameter_iterable, Sized):
                n_candidates = len(parameter_iterable)
                print("Fitting {0} folds for each of {1} candidates, totalling"
                      " {2} fits".format(len(cv), n_candidates,
                                         n_candidates * len(cv)))

        base_estimator = clone(self.estimator)

        pre_dispatch = self.pre_dispatch

        out = Parallel(
            n_jobs=self.n_jobs, verbose=self.verbose,
            pre_dispatch=pre_dispatch
        )(
            delayed(cv_fit_and_score)(clone(base_estimator), X, y, self.scoring,
                                      parameters, cv=cv)
            for parameters in parameter_iterable)

        #best = sorted(out, reverse=True)[0]
        best = sorted(out, key=lambda x: x[0], reverse=True)[0]

        self.best_params_ = best[1]
        self.best_score_ = best[0]

        if self.refit:
            # fit the best estimator using the entire dataset
            # clone first to work around broken estimators
            best_estimator = clone(base_estimator).set_params(
                **best[1])
            if y is not None:
                best_estimator.fit(X, y, **self.fit_params)
            else:
                best_estimator.fit(X, **self.fit_params)
            self.best_estimator_ = best_estimator

        return self


def decodeLabel(label):
    label = label[:2]  # Only the first 2 characters designate the label code

    mapping = {'c1': 0, 'c2': 1, 'c3': 1, 'c4': 0, 'c5': 0, 'c6': 0, 'c7': 2, }

    return mapping[label]



def checkStressMark(stressMark, pid, starttime):
    endtime = starttime + 60000  # One minute windows
    result = []
    for line in stressMark:
        [id, gt, st, et] = line

        if id == pid and (gt not in ['c7']):
            if (starttime > st) and (endtime < et):
                result.append(gt)

    data = Counter(result)
    return data.most_common(1)


def analyze_events_with_features(features, stress_marks):
    featureLabels = []
    finalFeatures = []
    subjects = []

    startTimes = {}
    for pid, label, start, end in stress_marks:
        if label == 'c4':
            if pid not in startTimes:
                startTimes[pid] = np.inf

            startTimes[pid] = min(startTimes[pid], start)

    for line in features:
        id = line[0]
        ts = line[1]
        f = line[2:]

        if ts < startTimes[id]:
            continue  # Outside of starting time

        label = checkStressMark(stress_marks, id, ts)
        if len(label) > 0:
            stressClass = decodeLabel(label[0][0])

            featureLabels.append(stressClass)
            finalFeatures.append(f)
            subjects.append(id)

    return finalFeatures, featureLabels, subjects


def get_svmdataset(traindata, trainlabels):
    input = []
    output = []
    foldinds = []

    for i in range(len(trainlabels)):
        if trainlabels[i] == 1:
            foldinds.append(i)

        if trainlabels[i] == 0:
            foldinds.append(i)

    input = np.array(input, dtype='float64')
    return output, input, foldinds


def reduceData(data, r):
    result = []
    for d in data:
        result.append([d[i] for i in r])
    return result


def f1Bias_scorer(estimator, X, y, ret_bias=False):
    probas_ = estimator.predict_proba(X)
    precision, recall, thresholds = metrics.precision_recall_curve(y, probas_[:, 1])

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


def svmOutput(filename, traindata, trainlabels):
    with open(filename, 'w') as f:
        for i in range(0, len(trainlabels)):
            f.write(str(trainlabels[i]))
            for fi in range(0, len(traindata[i])):
                f.write(" " + str(fi + 1) + ":" + str(traindata[i][fi]))

            f.write("\n")


def saveModel(filename, model, normparams, bias=0.5):
    class Object:
        def to_JSON(self):
            return json.dumps(self, default=lambda o: o.__dict__,
                              sort_keys=True, indent=4)

    class Kernel(Object):
        def __init__(self, type, parameters):
            self.type = type
            self.parameters = parameters

    class KernelParam(Object):
        def __init__(self, name, value):
            self.name = name;
            self.value = value

    class Support(Object):
        def __init__(self, dualCoef, supportVector):
            self.dualCoef = dualCoef
            self.supportVector = supportVector

    class NormParam(Object):
        def __init__(self, mean, std):
            self.mean = mean
            self.std = std

    class SVCModel(Object):
        def __init__(self, modelName, modelType, intercept, bias, probA, probB, kernel, support, normparams):
            self.modelName = modelName;
            self.modelType = modelType;
            self.intercept = intercept;
            self.bias = bias;
            self.probA = probA;
            self.probB = probB;
            self.kernel = kernel
            self.support = support
            self.normparams = normparams

    model = SVCModel('cStress', 'svc', model.intercept_[0], bias, model.probA_[0], model.probB_[0],
                     Kernel('rbf', [KernelParam('gamma', model._gamma)]),
                     [Support(model.dual_coef_[0][i], list(model.support_vectors_[i])) for i in
                      range(len(model.dual_coef_[0]))],
                     [NormParam(normparams.mean_[i], normparams.scale_[i]) for i in range(len(normparams.scale_))])

    with open(filename, 'w') as f:
        #print >> f, model.to_JSON()
        #print(model.to_JSON(), end="", file=f)
        f.write(model.to_JSON())

def cross_val_probs(estimator, X, y, cv):
    probs = np.zeros(len(y))

    for train, test in cv:
        temp = estimator.fit(X[train], y[train]).predict_proba(X[test])
        probs[test] = temp[:, 1]

    return probs

def readfile(filename):
    data = []
    with gzip.open(filename, 'rt') as f:
        count = 0
        for l in f:
            dp = parser.data_processor(l)
            if isinstance(dp, DataPoint):
                data.append(dp)
                count += 1
            if count > 20000:
                break
    return data

#added stress file reader by anjana
def read_stress_file(filename):

    stress_markers = []
    with gzip.open(filename, 'rt') as f:

        for l in f:
            parts = [x.strip() for x in l.split(',')]
            val = parts[0][:2]

            timestampBegin = datetime.fromtimestamp(float(parts[2]) / 1000.0, pytz.timezone('US/Central'))
            timestampEnd = datetime.fromtimestamp(float(parts[2]) / 1000.0, pytz.timezone('US/Central'))
            dp = DataPoint.from_tuple(start_time=timestampBegin, end_time=timestampEnd, sample=val)

            if isinstance(dp, DataPoint):
                stress_markers.append(dp)
    return stress_markers

#taken from main added stress file reader by anjana
def feature_target_loader(identifier: int):
    participant = "SI%02d" % identifier

    participant_uuid = uuid.uuid4()

    try:
        ecg = DataStream(None, participant_uuid)
        ecg.data = readfile(find(basedir, {"participant": participant, "datasource": "ecg"}))

        rip = DataStream(None, participant_uuid)
        rip.data = readfile(find(basedir, {"participant": participant, "datasource": "rip"}))

        accelx = DataStream(None, participant_uuid)
        accelx.data = readfile(find(basedir, {"participant": participant, "datasource": "accelx"}))

        accely = DataStream(None, participant_uuid)
        accely.data = readfile(find(basedir, {"participant": participant, "datasource": "accely"}))

        accelz = DataStream(None, participant_uuid)
        accelz.data = readfile(find(basedir, {"participant": participant, "datasource": "accelz"}))

        stress_markers = DataStream(None, participant_uuid)
        stress_markers.data = read_stress_file(find(basedir, {"participant": participant, "datasource": "stress_marks"}))

        return {"participant": participant, "ecg": ecg, "rip": rip, "accelx": accelx, "accely": accely,
                "accelz": accelz, "stress_markers": stress_markers}
    except Exception as e:
        print("File missing for %s" % participant)

        return {"ERROR": 'missing data file'}


#taken from main
def cStress_model_main():

    start_time = time.time()
    ids = CC.sparkSession.sparkContext.parallelize([i for i in range(1, 5)])

    data = ids.map(lambda i: feature_target_loader(i)).filter(lambda x: 'participant' in x)

    cstress_feature_vector = cStress(data)

    #pprint(cstress_feature_vector.collect())
    cStress_model(cstress_feature_vector, data)

    # results = ids.map(loader)
    # pprint(results.collect())
    end_time = time.time()
    print(end_time - start_time)

#features read as list from datastream generated by cStress by anjana
def read_features(features):
    participant_feature_list = []
    for participant_no in range(len(features)):
        participant_id = features[participant_no][0]
        types_of_feature_list = []
        for types_of_feature_index in range(len(features[participant_no][1])):
            sub_types_of_types_of_feature = list(features[participant_no][1][types_of_feature_index])
            sub_types_of_types_of_feature_list = []
            for sub_types_of_types_of_feature_index in range(len(sub_types_of_types_of_feature)):
                feature_val_array = np.array(['participant_no_id', 'start_time', 'end_time', 'sample'])
                for feature_val_index in range(len(sub_types_of_types_of_feature[sub_types_of_types_of_feature_index].data)):
                    feature_val_item = []
                    feature_val_item.append(participant_id)
                    feature_val_item.append(sub_types_of_types_of_feature[sub_types_of_types_of_feature_index].data[feature_val_index].start_time)
                    feature_val_item.append(sub_types_of_types_of_feature[sub_types_of_types_of_feature_index].data[feature_val_index].end_time)
                    feature_val_item.append(sub_types_of_types_of_feature[sub_types_of_types_of_feature_index].data[feature_val_index].sample)
                    feature_val_item_array = np.asarray(feature_val_item)
                    feature_val_array = np.vstack([feature_val_array, feature_val_item_array])
                sub_types_of_types_of_feature_list.append(feature_val_array)
            types_of_feature_list.append(sub_types_of_types_of_feature_list)
        participant_feature_list.append(types_of_feature_list)

    return participant_feature_list

#features read as dictionary from datastream generated by cStress by anjana
def read_features_dict(features):
    participant_feature_dict = {}
    for participant_no in range(len(features)):
        participant_id = features[participant_no][0]
        for types_of_feature_index in range(len(features[participant_no][1])):
            sub_types_of_types_of_feature = list(features[participant_no][1][types_of_feature_index])
            for sub_types_of_types_of_feature_index in range(len(sub_types_of_types_of_feature)):
                for feature_val_index in range(len(sub_types_of_types_of_feature[sub_types_of_types_of_feature_index].data)):
                    start_time = sub_types_of_types_of_feature[sub_types_of_types_of_feature_index].data[feature_val_index].start_time
                    if (participant_id, start_time) in participant_feature_dict:
                        participant_feature_dict[participant_id, start_time].append(sub_types_of_types_of_feature[sub_types_of_types_of_feature_index].data[feature_val_index].sample)
                    else:
                        participant_feature_dict[participant_id, start_time] = []
                        participant_feature_dict[participant_id, start_time].append(sub_types_of_types_of_feature[sub_types_of_types_of_feature_index].data[feature_val_index].sample)

    return participant_feature_dict

#printing feature list
def read_features_list(features_list):
    for i in range(len(features_list)):
        for j in range(len(features_list[i])):
            for k in range(len(features_list[i][j])):
                for l in range(len(features_list[i][j][k])):
                    print(features_list[i][j][k][l], "\n")
                print("new feature\n")


#changed the from main function to cStress_model
def cStress_model(feature_rdd: RDD, target: RDD) -> RDD:
    features = feature_rdd.collect()
    groundtruth_rdd = target.map(lambda ds: (ds['participant'], ds['stress_markers']))
    groundtruth = groundtruth_rdd.collect()

    # features_list = read_features(features)
    # read_features_list(features_list)

    features_dict = read_features_dict(features)
    # print(features_dict)



    # Original codes

    # traindata, trainlabels, subjects = analyze_events_with_features(features, groundtruth)
    #
    # traindata = np.asarray(traindata, dtype=np.float64)
    # trainlabels = np.asarray(trainlabels)
    #
    # normalizer = preprocessing.StandardScaler()
    # traindata = normalizer.fit_transform(traindata)
    #
    # lkf = LabelKFold(subjects, n_folds=len(np.unique(subjects)))
    #
    # delta = 0.1
    # parameters = {'kernel': ['rbf'],
    #               'C': [2 ** x for x in np.arange(-12, 12, 0.5)],
    #               'gamma': [2 ** x for x in np.arange(-12, 12, 0.5)],
    #               'class_weight': [{0: w, 1: 1 - w} for w in np.arange(0.0, 1.0, delta)]}
    #
    # svc = svm.SVC(probability=True, verbose=False, cache_size=2000)
    #
    # if args.scorer == 'f1':
    #     scorer = f1Bias_scorer_CV
    # else:
    #     scorer = Twobias_scorer_CV
    #
    # if args.whichsearch == 'grid':
    #     clf = ModifiedGridSearchCV(svc, parameters, cv=lkf, n_jobs=-1, scoring=scorer, verbose=1, iid=False)
    # else:
    #     clf = ModifiedRandomizedSearchCV(estimator=svc, param_distributions=parameters, cv=lkf, n_jobs=-1,
    #                                      scoring=scorer, n_iter=args.n_iter,
    #                                      verbose=1, iid=False)
    #
    # clf.fit(traindata, trainlabels)
    # pprint(clf.best_params_)
    #
    # CV_probs = cross_val_probs(clf.best_estimator_, traindata, trainlabels, lkf)
    # score, bias = scorer(CV_probs, trainlabels, True)
    # print(score, bias)
    # if not bias == []:
    #     saveModel(args.modelOutput, clf.best_estimator_, normalizer, bias)
    #
    #     n = len(trainlabels)
    #
    #     if args.scorer == 'f1':
    #         predicted = np.asarray(CV_probs >= bias, dtype=np.int)
    #         classified = range(n)
    #     else:
    #         classified = np.where(np.logical_or(CV_probs <= bias[0], CV_probs >= bias[1]))[0]
    #         predicted = np.asarray(CV_probs[classified] >= bias[1], dtype=np.int)
    #
    #     print("Cross-Subject (" + str(len(np.unique(subjects))) + "-fold) Validation Prediction")
    #     print("Accuracy: " + str(metrics.accuracy_score(trainlabels[classified], predicted)))
    #     print(metrics.classification_report(trainlabels[classified], predicted))
    #     print(metrics.confusion_matrix(trainlabels[classified], predicted))
    #     print("Lost: %d (%f%%)" % (n - len(classified), (n - len(classified)) * 1.0 / n))
    #     print("Subjects: " + str(np.unique(subjects)))
    # else:
    #     print("Results not good")
    return features_dict



cStress_model_main()