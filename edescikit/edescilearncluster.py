"""
Copyright 2021, Institute e-Austria, Timisoara, Romania
    http://www.ieat.ro/
Developers:
 * Gabriel Iuhasz, iuhasz.gabriel@info.uvt.ro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import joblib
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt
from sklearn import metrics
# from sklearn.datasets.samples_generator import make_blobs
from sklearn.preprocessing import StandardScaler
import pickle as pickle
import os
from util import str2Bool
import pandas as pd
from edelogger import logger
from datetime import datetime
import time
import sys
import glob
from sklearn.decomposition import SparsePCA, PCA
import pyod
from util import ut2hum, log_format
import shap


class SciCluster:
    def __init__(self,
                 modelDir,
                 pred_analysis=False):
        self.modelDir = modelDir
        self.pred_analysis = pred_analysis

    def dask_sdbscanTrain(self,
                          settings,
                          mname,
                          data,
                          scaler=None):
        '''
        :param data: -> dataframe with data
        :param settings: -> settings dictionary
        :param mname: -> name of serialized clusterer
        :param scaler: -> scaler to use on data
        :return: -> clusterer
        :example settings: -> {eps:0.9, min_samples:10, metric:'euclidean' ,
        algorithm:'auto, leaf_size:30, p:0.2, n_jobs:1}
        '''

        if scaler is None:
            logger.warning('[{}] : [WARN] Scaler not defined'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
        else:
            logger.info('[{}] : [INFO] Scaling data ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            data = scaler.fit_transform(data)

        if not settings or settings is None:
            logger.warning('[{}] : [WARN] No DBScan parameters defined using default'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            settings = {}
        else:
            for k, v in settings.items():
                logger.info('[{}] : [INFO] DBScan parameter {} set to {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), k, v))

        try:
            db = DBSCAN(**settings).fit(data)
        except Exception as inst:
            logger.error('[{}] : [INFO] Failed to instanciate DBScan with {} and {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(1)
        labels = db.labels_
        logger.info('[{}] : [INFO] DBScan labels: {} '.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), labels))
        n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
        logger.info('[{}] : [INFO] DBScan estimated number of clusters {} '.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), n_clusters_))
        self.__serializemodel(db, 'sdbscan', mname)
        return db

    def sdbscanTrain(self, settings,
                     mname,
                     data):
        '''
        :param data: -> dataframe with data
        :param settings: -> settings dictionary
        :param mname: -> name of serialized clusterer
        :return: -> clusterer
        :example settings: -> {eps:0.9, min_samples:10, metric:'euclidean' ,
        algorithm:'auto, leaf_size:30, p:0.2, n_jobs:1}
        '''
        for k, v in settings.items():
            logger.info('[%s] : [INFO] SDBSCAN %s set to %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), k, v)
        sdata = StandardScaler().fit_transform(data)
        try:
            db = DBSCAN(eps=float(settings['eps']), min_samples=int(settings['min_samples']), metric=settings['metric'],
                        algorithm=settings['algorithm'], leaf_size=int(settings['leaf_size']), p=float(settings['p']),
                        n_jobs=int(settings['n_jobs'])).fit(sdata)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot instanciate sDBSCAN with %s and %s',
                           datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            print("Error while  instanciating sDBSCAN with %s and %s" % (type(inst), inst.args))
            sys.exit(1)
        labels = db.labels_
        print(labels)
        n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
        print('Estimated number of clusters: %d' % n_clusters_)
        self.__serializemodel(db, 'sdbscan', mname)
        return db

    def dask_isolationForest(self, settings,
                             mname,
                             data
                             ):
        '''
        :param settings: -> settings dictionary
        :param mname: -> name of serialized clusterer
        :param scaler: -> scaler to use on data
        :return: -> isolation forest instance
        :example settings: -> {n_estimators:100, max_samples:100, contamination:0.1, bootstrap:False,
                        max_features:1.0, n_jobs:1, random_state:None, verbose:0}
        '''
        if not settings or settings is None:
            logger.warning('[{}] : [WARN] No IsolationForest parameters defined using defaults'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            # print(settings)
            settings = {}
        else:
            for k, v in settings.items():
                logger.info('[{}] : [INFO] IsolationForest parameter {} set to {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), k, v))
        try:

            clf = IsolationForest(**settings)
            # print(clf)
        except Exception as inst:
            logger.error('[{}] : [INFO] Failed to instanciate IsolationForest with {} and {}'.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(1)

        try:
            with joblib.parallel_backend('dask'):
                logger.info('[{}] : [INFO] Using Dask backend for IsolationForest'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                clf.fit(data)
        except Exception as inst:
            logger.error('[{}] : [ERROR] Failed to fit IsolationForest with {} and {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(1)

        predict = clf.predict(data)
        anoOnly = np.argwhere(predict == -1)
        logger.info('[{}] : [INFO] Found {} anomalies in training dataset of shape {}.'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), len(anoOnly), data.shape))
        logger.info('[{}] : [DEBUG] Predicted Anomaly Array {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), predict))
        self.__serializemodel(clf, 'isoforest', mname)
        self.__appendPredictions(method='isoforest', mname=mname, data=data, pred=predict)


    def isolationForest(self, settings,
                        mname,
                        data):
        '''
        :param settings: -> settings dictionary
        :param mname: -> name of serialized clusterer
        :return: -> isolation forest instance
        :example settings: -> {n_estimators:100, max_samples:100, contamination:0.1, bootstrap:False,
                        max_features:1.0, n_jobs:1, random_state:None, verbose:0}
        '''
        # rng = np.random.RandomState(42)
        if settings['random_state'] == 'None':
            settings['random_state'] = None

        if isinstance(settings['bootstrap'], str):
            settings['bootstrap'] = str2Bool(settings['bootstrap'])

        if isinstance(settings['verbose'], str):
            settings['verbose'] = str2Bool(settings['verbose'])

        if settings['max_samples'] != 'auto':
            settings['max_samples'] = int(settings['max_samples'])
        # print type(settings['max_samples'])
        for k, v in settings.items():
            logger.info('[%s] : [INFO] IsolationForest %s set to %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), k, v)
            print("IsolationForest %s set to %s" % (k, v))
        try:
            clf = IsolationForest(n_estimators=int(settings['n_estimators']), max_samples=settings['max_samples'], contamination=float(settings['contamination']), bootstrap=settings['bootstrap'],
                        max_features=float(settings['max_features']), n_jobs=int(settings['n_jobs']), random_state=settings['random_state'], verbose=settings['verbose'])
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot instanciate isolation forest with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(1)
        # clf = IsolationForest(max_samples=100, random_state=rng)
        # print "*&*&*&& %s" % type(data)
        try:
            clf.fit(data)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Cannot fit isolation forest model with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(1)
        predict = clf.predict(data)
        print("Anomaly Array:")
        print(predict)
        self.__serializemodel(clf, 'isoforest', mname)
        return clf

    def detect(self, method,
               model,
               data):
        '''
        :param method: -> method name
        :param model: -> trained clusterer
        :param data: -> dataframe with data
        :return: -> dictionary that contains the list of anomalous timestamps
        '''
        smodel = self.__loadClusterModel(method, model)
        anomalieslist = []
        if not smodel:
            dpredict = 0
        else:
            if data.shape[0]:
                if isinstance(smodel, IsolationForest):
                    logger.info('[{}] : [INFO] Loading predictive model IsolationForest ').format(
                        datetime.fromtimestamp(time.time()).strftime(log_format))
                    for k, v in smodel.get_params().items():
                        logger.info('[{}] : [INFO] Predict model parameter {} set to {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), k, v))
                    # print("Contamination -> %s" % smodel.contamination)
                    # print("Max_Features -> %s" % smodel.max_features)
                    # print("Max_Samples -> %s" % smodel.max_samples_)
                    # print("Threashold -> %s " % smodel.threshold_)
                    try:
                        dpredict = smodel.predict(data)
                        logger.debug('[{}] : [DEBUG] IsolationForest prediction array: {}').format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), str(dpredict))
                    except Exception as inst:
                        logger.error('[%s] : [ERROR] Error while fitting isolationforest model to event with %s and %s',
                             datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
                        dpredict = 0

                elif isinstance(smodel, DBSCAN):
                    logger.info('[{}] : [INFO] Loading predictive model DBSCAN ').format(
                        datetime.fromtimestamp(time.time()).strftime(log_format))
                    for k, v in smodel.get_params().items():
                        logger.info('[{}] : [INFO] Predict model parameter {} set to {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), k, v))
                    # print("Leaf_zise -> %s" % smodel.leaf_size)
                    # print("Algorithm -> %s" % smodel.algorithm)
                    # print("EPS -> %s" % smodel.eps)
                    # print("Min_Samples -> %s" % smodel.min_samples)
                    # print("N_jobs -> %s" % smodel.n_jobs)
                    try:
                        dpredict = smodel.fit_predict(data)
                    except Exception as inst:
                        logger.error('[%s] : [ERROR] Error while fitting sDBSCAN model to event with %s and %s',
                                     datetime.fromtimestamp(time.time()).strftime(log_format), type(inst),
                                     inst.args)
                        dpredict = 0
            else:
                dpredict = 0
                logger.warning('[%s] : [WARN] Dataframe empty with shape (%s,%s)',
                             datetime.fromtimestamp(time.time()).strftime(log_format), str(data.shape[0]),
                             str(data.shape[1]))
                print("Empty dataframe received with shape (%s,%s)" % (str(data.shape[0]),
                             str(data.shape[1])))
            print("dpredict type is %s" % (type(dpredict)))
        if type(dpredict) is not int:
            anomalyarray = np.argwhere(dpredict == -1)
            for an in anomalyarray:
                anomalies = {}
                anomalies['utc'] = int(data.iloc[an[0]].name)
                anomalies['hutc'] = ut2hum(int(data.iloc[an[0]].name))
                anomalieslist.append(anomalies)
        anomaliesDict = {}
        anomaliesDict['anomalies'] = anomalieslist
        logger.info('[%s] : [INFO] Detected anomalies with model %s using method %s are -> %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), model, method, str(anomaliesDict))
        return anomaliesDict

    def dask_detect(self,
                    method,
                    model,
                    data,
                    ):
        smodel = self.__loadClusterModel(method, model)
        anomaliesList = []
        anomaliesDict = {}
        shap_values_p = 0
        if not smodel:
            dpredict = 0
        else:
            if data.shape[0]:
                try:
                    logger.info('[{}] : [INFO] Loading predictive model {} '.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), str(smodel).split('(')[0]))
                    for k, v in smodel.get_params().items():
                        logger.info('[{}] : [INFO] Predict model parameter {} set to {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), k, v))
                        dpredict = smodel.predict(data)
                except Exception as inst:
                    logger.error('[{}] : [ERROR] Failed to load predictive model with {} and {}'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
                    dpredict = 0
            else:
                dpredict = 0
                logger.warning('[{}] : [WARN] DataFrame is empty with shape {} '.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), str(data.shape)))
        from pyod.models.iforest import IForest  # pyod.models.iforest.IForest
        if list(np.unique(dpredict)) == [0, 1] or isinstance(smodel, IForest):
            anomaly_label = 1
        else:
            anomaly_label = -1

        if type(dpredict) is not int:
            anomalyArray = np.argwhere(dpredict == anomaly_label)
            if self.pred_analysis and anomalyArray.shape[0]:
                try:
                    plot = self.pred_analysis['Plot']
                    # print(self.pred_analysis['Plot'])
                except Exception:
                    plot = False
                feature_importance, shap_values = self.__shap_analysis(model=smodel, data=data, plot=plot)
                anomaliesDict['complete_shap_analysis'] = feature_importance
                shap_values_p = 1
            count = 0
            for an in anomalyArray:
                anomalies = {}
                if pd._libs.tslibs.timestamps.Timestamp == type(data.iloc[an[0]].name):
                    anomalies['utc'] = data.iloc[an[0]].name.timestamp()
                    anomalies['hutc'] = str(data.iloc[an[0]].name)
                else:
                    anomalies['utc'] = int(data.iloc[an[0]].name)
                    anomalies['hutc'] = ut2hum(int(data.iloc[an[0]].name))
                if shap_values_p:
                    anomalies['analysis'] = self.__shap_force_layout(shap_values=shap_values,
                                                                     instance=count)
                anomaliesList.append(anomalies)
                count += 1

        anomaliesDict['anomalies'] = anomaliesList
        logger.info('[{}] : [INFO] Detected {} anomalies with model {} using method {} '.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), len(anomaliesList), model,
            str(smodel).split('(')[0]))
        return anomaliesDict

    def __shap_analysis(self,
                        model,
                        data,
                        plot=False):
        """
        Execute shapely value calculation on incoming data and model prediction.
        Several plots are also calculated if set: heatmap, summary and feature importance.

        :param model: Predictive model (only for binary classification)
        :param data: Data collected from monitoring or local file defined in by the user
        :param plot: If set to True each query interval will also generate the above mentioned plots.
        :return: feature importance dictionary form (from pandas dataframe), shapely values
        """
        explainer = shap.Explainer(model, data)
        shap_values = explainer(data)
        vals = np.abs(shap_values.values).mean(0)
        feature_importance = pd.DataFrame(list(zip(shap_values.feature_names, vals)),
                                          columns=['feature_name', 'feature_importance_vals'])
        feature_importance.sort_values(by=['feature_importance_vals'], ascending=False, inplace=True)
        if plot:
            self.__shap_heatmap(shap_values=shap_values)
            self.__shap_summary(shap_values=shap_values, data=data)
            self.__shap_feature_importance(shap_values=shap_values)
        return feature_importance.to_dict(), shap_values

    def __shap_force_layout(self,
                            shap_values,
                            instance):
        """
        Computes forced layout similar to force_plot from shap.
        This is done on a per event/detection (i.e. row) basis

        :param shap_values: Shapley values
        :param instance: feature instance as used in df.iloc
        :return: shap_values on a per detection instance basis
        """
        shap_values_d = {}
        try:
            shap_values_d['shap_values'] = dict(zip(shap_values.feature_names, shap_values[instance].values))
            shap_values_d['base_values'] = shap_values[instance].base_values
        except Exception as inst:
            logger.error('[{}] : [ERROR] Error while executing shap processing with {} and {} '.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
        return shap_values_d

    def __shap_heatmap(self,
                       shap_values):
        """
        Passing a matrix of SHAP values to the heatmap plot function creates a plot with the instances on the x-axis,
        the model inputs on the y-axis, and the SHAP values encoded on a color scale.

        :param shap_values: Shapley values.
        :return:
        """
        shap.plots.heatmap(shap_values, show=False)
        shap_heatmap_f = f"IForest_heatmap_prediction_{time.time()}.png"
        plt.savefig(os.path.join(self.modelDir, shap_heatmap_f), bbox_inches="tight")
        plt.close()

    def __shap_summary(self,
                       shap_values,
                       data):
        """
        Summary plots are a different representation of feature importance


        :param shap_values: Shapley values.
        :param data: Data collected from monitoring or local file defined in by the user
        :return:
        """
        shap.summary_plot(shap_values, data, plot_type="violin", show=False)
        shap_summary_f = f"IForest_summary_prediction_{time.time()}.png"
        plt.savefig(os.path.join(self.modelDir, shap_summary_f), bbox_inches="tight")
        plt.close()

    def __shap_feature_importance(self,
                                  shap_values,
                                  max_display=30):
        """
        Feature importance for current events

        :param shap_values: Shapley values.
        :param max_display: Maximum features to display
        :return:
        """
        shap.plots.bar(shap_values, max_display=max_display, show=False)
        shap_feature_f = f"IForest_feature_importance_prediction_{time.time()}.png"
        plt.savefig(os.path.join(self.modelDir, shap_feature_f), bbox_inches="tight")
        plt.close()

    def dask_clusterMethod(self, cluster_method,
                           mname,
                           data
                           ):
        try:
            logger.info('[{}] : [INFO] Loading Clustering method {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), type(cluster_method)))
            # delattr(cluster_method, 'behaviour')
            # del cluster_method.__dict__['behaviour']
            for k, v in cluster_method.get_params().items():
                logger.info('[{}] : [INFO] Method parameter {} set to {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), k, v))
            try:
                with joblib.parallel_backend('dask'):
                    logger.info('[{}] : [INFO] Using Dask backend for user defined method'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format)))
                    clf = cluster_method.fit(data)
            except Exception as inst:
                logger.error('[{}] : [ERROR] Failed to fit user defined method with dask backend with {} and {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
                logger.warning('[{}] : [WARN] using default process based backend for user defined method'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
                clf = cluster_method.fit(data)
        except Exception as inst:
            logger.error('[{}] : [ERROR] Failed to fit {} with {} and {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), type(cluster_method),
                type(inst), inst.args))
            sys.exit(1)
        predictions = clf.predict(data)
        if list(np.unique(predictions)) == [0, 1]:
            anomaly_marker = 1
            normal_marker = 0
        else:
            anomaly_marker = -1
            normal_marker = 1
        logger.info('[{}] : [INFO] Number of Predicted Anomalies {} from a total of {} datapoints.'.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), list(predictions).count(anomaly_marker), len(list(predictions))))
        logger.debug('[{}] : [DEBUG] Predicted Anomaly Array {}'.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), predictions))
        fname = str(clf).split('(')[0]
        self.__serializemodel(clf, fname, mname)
        self.__plot_feature_sep(data, predictions, method=fname, mname=mname, anomaly_label=anomaly_marker,
                                normal_label=normal_marker)
        self.__decision_boundary(clf, data, method=fname, mname=mname,anomaly_label=anomaly_marker)

        return clf

    def __appendPredictions(self, method, mname, data, pred):
        fpath = "{}_{}.csv".format(method, mname)
        fname = os.path.join(self.modelDir, fpath)
        logger.info('[{}] : [INFO] Appending predictions to data ... Saving to {}.'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), fname))
        data['ano'] = pred
        data.to_csv(fname, index=True)

    def __serializemodel(self, model,
                         method,
                         mname):
        '''
        :param model: -> model
        :param method: -> method name
        :param mname: -> name to be used for saved model
        :result: -> Serializez current clusterer/classifier
        '''
        fpath = "%s_%s.pkl" % (method, mname)
        fname = os.path.join(self.modelDir, fpath)
        pickle.dump(model, open(fname, "wb"))
        logger.info('[{}] : [INFO] Serializing model {} at {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), method, fpath))

    def __loadClusterModel(self, method,
                           model):
        '''
        :param method: -> method name
        :param model: -> model name
        :return: -> instance of serialized object
        '''
        lmodel = glob.glob(os.path.join(self.modelDir, ("%s_%s.pkl" % (method, model))))
        if not lmodel:
            logger.warning('[%s] : [WARN] No %s model with the name %s found',
                         datetime.fromtimestamp(time.time()).strftime(log_format), method, model)
            return 0
        else:
            smodel = pickle.load(open(lmodel[0], "rb"))
            logger.info('[%s] : [INFO] Succesfully loaded %s model with the name %s',
                        datetime.fromtimestamp(time.time()).strftime(log_format), method, model)
            return smodel

    def __decision_boundary(self,
                            model,
                            data,
                            method,
                            mname,
                            anomaly_label=-1,
                            ):
        """
        :param model: model to be refitted with 2 features (PCA)
        :param data: dataset after PCA
        :param method: method used for plotting decision boundary
        :param mname: name of the model to be displayed
        :param anomaly_label: label for anomaly instances (differs from method to method)
        """
        logger.info('[{}] : [INFO] Computing PCA with 2 components for decision boundary ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
        transformer = PCA(n_components=2)
        transformer.fit(data)
        data = transformer.transform(data)
        # print("PCA data shape: {}".format(data.shape))
        # fit model
        try:
            model.set_params(
                max_features=data.shape[-1])  # becouse we have only two features we must override previous setting
        except ValueError:
            logger.debug('[{}] : [Debug] Model not effected by max feature parameter, setting encoding and decoding size'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            model.set_params(
                encoder_neurons=[2, 64, 32],
                decoder_neurons=[32, 64, 2]
            )

        model.fit(data)
        y_pred_outliers = model.predict(data)

        # get anomaly index
        anomaly_index_rf = np.where(y_pred_outliers == anomaly_label)

        # Get anomalies based on index
        ano_rf = data[anomaly_index_rf]
        # plot the line, the samples, and the nearest vectors to the plane
        xx, yy = np.meshgrid(np.linspace(-15, 25, 80), np.linspace(-5, 20, 80))
        Z = model.decision_function(np.c_[xx.ravel(), yy.ravel()])
        Z = Z.reshape(xx.shape)
        plt.title(f"Decision Boundary for {method} with name {mname}")
        plt.contourf(xx, yy, Z, cmap=plt.cm.Blues_r)
        plt.contour(xx, yy, Z, levels=[0], linewidths=2, colors='black')
        b1 = plt.scatter(data[:, 0], data[:, 1], c='white',
                         s=20, edgecolor='k')
        c = plt.scatter(ano_rf[:, 0], ano_rf[:, 1], c='red',
                        s=20, edgecolor='k')
        plt.axis('tight')
        plt.xlim((-15, 25))
        plt.ylim((-5, 20))
        plt.legend([b1, c],
                   ["normal",
                    "anomaly", ],
                   loc="upper left")
        plot_name = f"Decision_Boundary_{method}_{mname}.png"
        plt.savefig(os.path.join(self.modelDir, plot_name))
        plt.close();

    def __plot_feature_sep(self,
                           data,
                           pred,
                           method,
                           mname,
                           anomaly_label=-1,
                           normal_label=1,
                           limit=10):
        """
        :param data: dataset used for training or prediction
        :param pred: model prediction
        :param method: method used for plotting decision boundary
        :param mname: name of the model to be displayed
        :param anomaly_label: label for anomaly instances (differs from method to method)
        :param normal_label: labal of normal data
        :param limit: limits the number of features to be plotted # todo add limit to cfg file
        :return:
        """
        col_names_plt = list(data.columns.values)
        data['anomaly'] = pred
        i = 0
        for feature in col_names_plt:
            if i > limit:
                break
            if feature == 'time' or feature == 'anomaly':
                pass
            else:
                normal_event = data.loc[data['anomaly'] == normal_label, feature]
                anomay_event = data.loc[data['anomaly'] == anomaly_label, feature]
                plt.figure(figsize=(10, 6))
                plt.hist(normal_event, bins=50, alpha=0.5, density=True, label='{} normal'.format(feature))
                plt.hist(anomay_event, bins=50, alpha=0.5, density=True, label='{} anomaly'.format(feature))
                plt.legend(loc='upper right')
                plot_name = f"Feature_Separation_{method}_{mname}_{feature}.png"
                plt.savefig(os.path.join(self.modelDir, plot_name))
                plt.close();
            i+=1
