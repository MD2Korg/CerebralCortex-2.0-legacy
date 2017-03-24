
# Copyright (c) 2016, MD2K Center of Excellence
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
from pathlib import Path
from collections import Sized
from collections import Counter
import numpy as np
from sklearn import svm, metrics, preprocessing
from sklearn.base import clone, is_classifier
from sklearn.cross_validation import LabelKFold, check_cv, _fit_and_score
from sklearn.grid_search import GridSearchCV, RandomizedSearchCV, ParameterSampler, ParameterGrid
from sklearn.grid_search import _check_param_grid, _CVScoreTuple
from sklearn.utils.validation import _num_samples, indexable
from sklearn.metrics.scorer import check_scoring
from pyspark.sql import SparkSession
from pyspark import SparkContext


# Command line parameter configuration
parser = argparse.ArgumentParser(description='Train and evaluate the cStress model')
parser.add_argument('--featureFolder', dest='featureFolder', required=True,
                    help='Directory containing feature files')
parser.add_argument('--scorer', type=str, required=True, dest='scorer',
                    help='Specify which scorer function to use (f1 or twobias)')
parser.add_argument('--whichsearch', type=str, required=True, dest='whichsearch',
                    help='Specify which search function to use (GridSearch or RandomizedSearch')
parser.add_argument('--n_iter', type=int, required=False, dest='n_iter',
                    help='If Randomized Search is used, how many iterations to use')
parser.add_argument('--modelOutput', type=str, required=True, dest='modelOutput',
                    help='Model file to write')
parser.add_argument('--featureFile', type=str, required=True, dest='featureFile',
                    help='Feature vector file name')
parser.add_argument('--stressFile', type=str, required=True, dest='stressFile',
                    help='Stress ground truth filename')
args = parser.parse_args()

sc = SparkContext()


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


def decodeLabel(label):
    label = label[:2]  # Only the first 2 characters designate the label code

    mapping = {'c1': 0, 'c2': 1, 'c3': 1, 'c4': 0, 'c5': 0, 'c6': 0, 'c7': 2, }

    return mapping[label]


def readFeatures(folder, filename):
    features = []

    path = Path(folder)
    files = list(path.glob('**/' + filename))

    for f in files:
        participantID = int(f.parent.name[2:])
        with f.open() as file:
            for line in file.readlines():
                parts = [x.strip() for x in line.split(',')]

                featureVector = [participantID, int(parts[0])]
                featureVector.extend([float(p) for p in parts[1:]])

                features.append(featureVector)
    return features


def readStressmarks(folder, filename):
    features = []

    path = Path(folder)
    files = list(path.glob('**/' + filename))

    for f in files:
        participantID = int(f.parent.name[2:])

        with f.open() as file:
            for line in file.readlines():
                parts = [x.strip() for x in line.split(',')]
                label = parts[0][:2]
                features.append([participantID, label, int(parts[2]), int(parts[3])])

    return features


def checkStressMark(stressMark, pid, starttime):
    endtime = starttime + 60000  # One minute windows
    result = []
    for line in stressMark:
        [id_index, gt, st, et] = line

        if id_index == pid and (gt not in ['c7']):
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
        id_index = line[0]
        ts = line[1]
        f = line[2:]

        if ts < startTimes[id_index]:
            continue  # Outside of starting time

        label = checkStressMark(stress_marks, id_index, ts)
        if len(label) > 0:
            stressClass = decodeLabel(label[0][0])

            featureLabels.append(stressClass)
            finalFeatures.append(f)
            subjects.append(id_index)

    return finalFeatures, featureLabels, subjects


def get_svmdataset(traindata, trainlabels):
    input_data = []
    output_data = []
    foldinds_val = []

    for i in range(len(trainlabels)):
        if trainlabels[i] == 1:
            foldinds_val.append(i)

        if trainlabels[i] == 0:
            foldinds_val.append(i)

    input_data = np.array(input_data, dtype='float64')
    return output_data, input_data, foldinds_val


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
        def __init__(self, type_val, parameters):
            self.type_val = type_val
            self.parameters = parameters

    class KernelParam(Object):
        def __init__(self, name, value):
            self.name = name
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
            self.modelName = modelName
            self.modelType = modelType
            self.intercept = intercept
            self.bias = bias
            self.probA = probA
            self.probB = probB
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


class GridSearchCVSparkParallel(GridSearchCV):

    def __init__(self, sc, estimator, param_grid, scoring=None,
                 fit_params=None, n_jobs=1, iid=True, refit=True, cv=None, verbose=0,
                 pre_dispatch='2*n_jobs', error_score='raise'):
        super(GridSearchCVSparkParallel, self).__init__(
            estimator=estimator, param_grid=param_grid, scoring=scoring,
            fit_params=fit_params, n_jobs=n_jobs, iid=iid, refit=refit, cv=cv, verbose=verbose,
            pre_dispatch=pre_dispatch, error_score=error_score)
        self.sc = sc
        self.param_grid = param_grid
        #self.grid_scores_ = None
        _check_param_grid(param_grid)

    def fit(self, X, y):
        """Actual fitting,  performing the search over parameters."""

        estimator = self.estimator
        cv = self.cv
        self.scorer_ = check_scoring(self.estimator, scoring=self.scoring)
        param_grid = self.param_grid

        n_samples = _num_samples(X)
        X, y = indexable(X, y)

        parameter_iterable = ParameterGrid(param_grid)
        print(parameter_iterable)

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
        #pre_dispatch = self.pre_dispatch

        param_grid = [(parameters, train, test)
                      for parameters in parameter_iterable
                      for (train, test) in cv]


        # Because the original python code expects a certain order for the elements
        indexed_param_grid = list(zip(range(len(param_grid)), param_grid))
        par_param_grid = self.sc.parallelize(indexed_param_grid, len(indexed_param_grid))
        X_bc = self.sc.broadcast(X)
        y_bc = self.sc.broadcast(y)

        scorer = self.scorer_
        verbose = self.verbose
        fit_params = self.fit_params
        error_score = self.error_score
        fas = _fit_and_score

        def fun(tup):
            (index, (parameters, train, test)) = tup
            local_estimator = clone(base_estimator)
            local_X = X_bc.value
            local_y = y_bc.value
            res = fas(local_estimator, local_X, local_y, scorer, train, test, verbose,
                      parameters, fit_params,
                      return_parameters=True, error_score=error_score)
            return (index, res)
        indexed_out0 = dict(par_param_grid.map(fun).collect())
        out = [indexed_out0[idx] for idx in range(len(param_grid))]

        X_bc.unpersist()
        y_bc.unpersist()

        # Out is a list of triplet: score, estimator, n_test_samples
        n_fits = len(out)
        n_folds = len(cv)

        scores = list()
        grid_scores = list()
        for grid_start in range(0, n_fits, n_folds):
            n_test_samples = 0
            score = 0
            all_scores = []
            for this_score, this_n_test_samples, _, parameters in \
                    out[grid_start:grid_start + n_folds]:
                all_scores.append(this_score)
                if self.iid:
                    this_score *= this_n_test_samples
                    n_test_samples += this_n_test_samples
                score += this_score
            if self.iid:
                score /= float(n_test_samples)
            else:
                score /= float(n_folds)
            scores.append((score, parameters))
            # TODO: shall we also store the test_fold_sizes?
            grid_scores.append(_CVScoreTuple(
                parameters,
                score,
                np.array(all_scores)))
        # Store the computed scores
        self.grid_scores_ = grid_scores

        # Find the best parameters by comparing on the mean validation score:
        # note that `sorted` is deterministic in the way it breaks ties
        best = sorted(grid_scores, key=lambda x: x.mean_validation_score,
                      reverse=True)[0]
        self.best_params_ = best.parameters
        self.best_score_ = best.mean_validation_score

        if self.refit:
            # fit the best estimator using the entire dataset
            # clone first to work around broken estimators
            best_estimator = clone(base_estimator).set_params(
                **best.parameters)
            if y is not None:
                best_estimator.fit(X, y, **self.fit_params)
            else:
                best_estimator.fit(X, **self.fit_params)
            self.best_estimator_ = best_estimator
        return self



class RandomGridSearchCVSparkParallel(RandomizedSearchCV):

    def __init__(self, sc, estimator, param_distributions, n_iter, scoring=None, fit_params=None,
                 n_jobs=1, iid=True, refit=True, cv=None, verbose=0,
                 pre_dispatch='2*n_jobs', random_state=None, error_score='raise'):
        super(RandomGridSearchCVSparkParallel, self).__init__(
            estimator=estimator, param_distributions=param_distributions, n_iter=n_iter, scoring=scoring, random_state=random_state,
            fit_params=fit_params, n_jobs=n_jobs, iid=iid, refit=refit, cv=cv, verbose=verbose,
            pre_dispatch=pre_dispatch, error_score=error_score)

        self.sc = sc
        self.param_distributions = param_distributions
        self.n_iter = n_iter
        # self.grid_scores_ = None
        # _check_param_grid(param_distributions)

    def fit(self, X, y):
        """Actual fitting,  performing the search over parameters."""

        estimator = self.estimator
        cv = self.cv
        self.scorer_ = check_scoring(self.estimator, scoring=self.scoring)
        n_samples = _num_samples(X)
        X, y = indexable(X, y)
        parameter_iterable = ParameterSampler(self.param_distributions, self.n_iter, random_state=self.random_state)

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
        #pre_dispatch = self.pre_dispatch

        param_grid = [(parameters, train, test)
                      for parameters in parameter_iterable
                      for (train, test) in cv]
        # Because the original python code expects a certain order for the elements, we need to
        # respect it.
        indexed_param_grid = list(zip(range(len(param_grid)), param_grid))
        par_param_grid = self.sc.parallelize(indexed_param_grid, len(indexed_param_grid))
        X_bc = self.sc.broadcast(X)
        y_bc = self.sc.broadcast(y)

        scorer = self.scorer_
        verbose = self.verbose
        fit_params = self.fit_params
        error_score = self.error_score
        fas = _fit_and_score


        def fun(tup):
            (index, (parameters, train, test)) = tup
            local_estimator = clone(base_estimator)
            local_X = X_bc.value
            local_y = y_bc.value
            #cv_fit_and_score(estimator, X, y, scorer, parameters, cv, ):
            res = fas(local_estimator, local_X, local_y, scorer, train, test, verbose,
                      parameters, fit_params,
                      return_parameters=True, error_score=error_score)
            return (index, res)


        indexed_out0 = dict(par_param_grid.map(fun).collect())
        out = [indexed_out0[idx] for idx in range(len(param_grid))]

        X_bc.unpersist()
        y_bc.unpersist()

        # Out is a list of triplet: score, estimator, n_test_samples
        n_fits = len(out)
        n_folds = len(cv)

        scores = list()
        grid_scores = list()

        for grid_start in range(0, n_fits, n_folds):
            n_test_samples = 0
            score = 0
            all_scores = []
            for this_score, this_n_test_samples, _, parameters in \
                    out[grid_start:grid_start + n_folds]:
                all_scores.append(this_score)
                if self.iid:
                    this_score *= this_n_test_samples
                    n_test_samples += this_n_test_samples
                score += this_score
            if self.iid:
                score /= float(n_test_samples)
            else:
                score /= float(n_folds)
            scores.append((score, parameters))
            # TODO: shall we also store the test_fold_sizes?
            grid_scores.append(_CVScoreTuple(
                parameters,
                score,
                np.array(all_scores)))
        # Store the computed scores
        self.grid_scores_ = grid_scores

        # Find the best parameters by comparing on the mean validation score:
        # note that `sorted` is deterministic in the way it breaks ties
        best = sorted(grid_scores, key=lambda x: x.mean_validation_score,
                      reverse=True)[0]
        self.best_params_ = best.parameters
        self.best_score_ = best.mean_validation_score

        if self.refit:
            # fit the best estimator using the entire dataset
            # clone first to work around broken estimators
            best_estimator = clone(base_estimator).set_params(
                **best.parameters)
            if y is not None:
                best_estimator.fit(X, y, **self.fit_params)
            else:
                best_estimator.fit(X, **self.fit_params)
            self.best_estimator_ = best_estimator
        return self


def cstress_model():
    features = readFeatures(args.featureFolder, args.featureFile)
    groundtruth = readStressmarks(args.featureFolder, args.stressFile)

    traindata, trainlabels, subjects = analyze_events_with_features(features, groundtruth)

    traindata = np.asarray(traindata, dtype=np.float64)
    trainlabels = np.asarray(trainlabels)

    normalizer = preprocessing.StandardScaler()
    traindata = normalizer.fit_transform(traindata)

    lkf = LabelKFold(subjects, n_folds=len(np.unique(subjects)))

    #Original Parameters of cStress Model
    # delta = 0.1
    # parameters = {'kernel': ['rbf'],
    #               'C': [2 ** x for x in np.arange(-12, 12, 0.5)],
    #               'gamma': [2 ** x for x in np.arange(-12, 12, 0.5)],
    #               'class_weight': [{0: w, 1: 1 - w} for w in np.arange(0.0, 1.0, delta)]}


    #parameters for testing
    delta = 0.5
    parameters = {'kernel': ['rbf'],
              'C': [2 ** x for x in np.arange(-1, 1, 0.5)],
              'gamma': [2 ** x for x in np.arange(-1, 1, 0.5)],
              'class_weight': [{0: w, 1: 1 - w} for w in np.arange(0.0, 1.0, delta)]}


    svc = svm.SVC(probability=True, verbose=False, cache_size=2000)

    if args.scorer == 'f1':
        scorer = f1Bias_scorer_CV
    else:
        scorer = Twobias_scorer_CV

    if args.whichsearch == 'grid':
        clf = GridSearchCVSparkParallel(sc=sc, estimator=svc, param_grid=parameters, cv=lkf, n_jobs=-1,
                                   scoring=None, verbose=1, iid=False)
    else:
        clf = RandomGridSearchCVSparkParallel(sc, estimator=svc, param_distributions=parameters, cv=lkf, n_jobs=-1,
                                 scoring=None, n_iter=args.n_iter,
                                 verbose=1, iid=False)

    clf.fit(traindata, trainlabels)

    sc.stop(); SparkSession._instantiatedContext = None

    print("best score: ", clf.best_score_)
    print("best params: ", clf.best_params_)

    CV_probs = cross_val_probs(clf.best_estimator_, traindata, trainlabels, lkf)
    score, bias = scorer(CV_probs, trainlabels, True)
    print("score and bias: ", score, bias)

    if not bias == []:
        saveModel(args.modelOutput, clf.best_estimator_, normalizer, bias)

        n = len(trainlabels)

        if args.scorer == 'f1':
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
        print("Results not good")




#cstress_model()