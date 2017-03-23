"""
Class for parallelizing GridSearchCV jobs in scikit-learn
"""

from collections import Sized
import numpy as np
from pyspark.sql import SparkSession
from sklearn import datasets, svm

from sklearn.base import BaseEstimator, is_classifier, clone
from sklearn.cross_validation import KFold, check_cv, _fit_and_score, _safe_split, train_test_split
from sklearn.grid_search import BaseSearchCV, _check_param_grid, ParameterGrid, _CVScoreTuple, ParameterSampler
from sklearn.metrics.scorer import check_scoring
from sklearn.utils.validation import _num_samples, indexable
from spark_sklearn.util import createLocalSparkSession

from pyspark import SparkContext, SparkConf
# conf = SparkConf().setAppName("aaa").setMaster("aaa")
sc = SparkContext()

class GridSearchCV(BaseSearchCV):

    def __init__(self, sc, estimator, param_grid, scoring=None, fit_params=None,
                 n_jobs=1, iid=True, refit=True, cv=None, verbose=0,
                 pre_dispatch='2*n_jobs', error_score='raise'):
        super(GridSearchCV, self).__init__(
            estimator, scoring, fit_params, n_jobs, iid,   #add param_grid
            refit, cv, verbose, pre_dispatch, error_score)
        self.sc = sc
        self.param_grid = param_grid
        self.grid_scores_ = None
        _check_param_grid(param_grid)

    def fit(self, X, y=None):
        """Run fit with all sets of parameters.
        Parameters
        ----------
        X : array-like, shape = [n_samples, n_features]
            Training vector, where n_samples is the number of samples and
            n_features is the number of features.
        y : array-like, shape = [n_samples] or [n_samples, n_output], optional
            Target relative to X for classification or regression;
            None for unsupervised learning.
        """
        return self._fit(X, y, ParameterGrid(self.param_grid))

    def _fit(self, X, y, parameter_iterable):
        """Actual fitting,  performing the search over parameters."""

        estimator = self.estimator
        cv = self.cv
        self.scorer_ = check_scoring(self.estimator, scoring=self.scoring)

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

class RandomGridSearchCV(BaseSearchCV):

    def __init__(self, sc, estimator, param_grid, n_iter=10, scoring=None, fit_params=None,
                 n_jobs=1, iid=True, refit=True, cv=None, verbose=0,
                 pre_dispatch='2*n_jobs', error_score='raise'):
        super(RandomGridSearchCV, self).__init__(
            estimator, scoring, fit_params, n_jobs, iid,
            refit, cv, verbose, pre_dispatch, error_score)
        self.sc = sc
        self.param_grid = param_grid
        self.grid_scores_ = None
        _check_param_grid(param_grid)

    def fit(self, X, y=None):
        """Run fit with all sets of parameters.
        Parameters
        ----------
        X : array-like, shape = [n_samples, n_features]
            Training vector, where n_samples is the number of samples and
            n_features is the number of features.
        y : array-like, shape = [n_samples] or [n_samples, n_output], optional
            Target relative to X for classification or regression;
            None for unsupervised learning.
        """
        return self._fit(X, y, ParameterGrid(self.param_grid))

    def _fit(self, X, y, parameter_iterable):
        """Actual fitting,  performing the search over parameters."""

        estimator = self.estimator
        cv = self.cv
        self.scorer_ = check_scoring(self.estimator, scoring=self.scoring)

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


####################################################################
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

###############################################################################
def c():
    iris = datasets.load_iris()
    X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target, test_size=0.3, random_state=0)
    delta = 0.5
    parameters = {'kernel': ['rbf'],
                  'C': [2 ** x for x in np.arange(-2, 2, 0.5)],
                  'gamma': [2 ** x for x in np.arange(-2, 2, 0.5)],
                  'class_weight': [{0: w, 1: 1 - w} for w in np.arange(0.0, 1.0, delta)]}

    spark = createLocalSparkSession()
    # iris = datasets.load_iris()
    #parameters = {'kernel':('linear', 'rbf'), 'C':[1, 10]}
    scorer = Twobias_scorer_CV
    svr = svm.SVC()
    clf = RandomGridSearchCV(sc, svr, parameters, scoring=None, n_jobs=-1, refit=True, verbose=1)
    clf.fit(X_train, y_train)
    spark.stop(); SparkSession._instantiatedContext = None

    clf.predict(X_test)
    print(clf.best_score_)
    print(clf.best_params_)


c()