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
from edeconnector import Connector, logger, datetime, time
from edepoint.edepoint import EdePoint
from util import queryParser, nodesParse, str2Bool, cfilterparse, rfilterparse, pointThraesholds, parseDelay, parseMethodSettings, ut2hum, checkFile, log_format
from .threadRun import EdeDetectThread, EdePointThread, EdeTrainThread
from .multiprocRun import EdeDetectProcess, EdePointProcess, EdeTrainProcess
from time import sleep
import sys
import os
import tempfile
from edescikit import edescilearncluster as sede
from edescikit import edescilearnclassification as cede
from pyQueryConstructor import QueryConstructor
from dataformatter import DataFormatter
import subprocess


class EDEngine:
    def __init__(self,
                 settingsDict,
                 dataDir,
                 modelsDir,
                 queryDir):
        self.esendpoint = settingsDict['esendpoint']
        self.prendoint = settingsDict['prendpoint']
        self.sr_pmds_end = settingsDict['pmdsendpoint'] # Serrano
        self.sr_pmds_check = False # Serrano, used for checking if PMDS is online, once on startup
        self.sr_cluster_id = settingsDict['pmdsclusterid'] # Serrano
        self.sr_pmds_namespace = settingsDict['pmdsnamespace'] # Serrano
        self.sr_pmds_group = settingsDict['pmdsgroups'] # Serrano
        self.sr_pmds_start = settingsDict['pmdsstart'] # Serrano
        self.sr_pmds_end = settingsDict['pmdsend'] # Serrano
        self.sr_pmds_field_measurements = settingsDict['pmdsmetrics'] # Serrano
        self.daskScheduler = settingsDict['Dask']['SchedulerEndpoint']
        self.daskSchedulerPort = settingsDict['Dask']['SchedulerPort']
        self.daskEnforce = settingsDict['Dask']['EnforceCheck']
        self.MPort = settingsDict['MPort']
        self.edepoint = EdePoint(settingsDict['esendpoint'])
        self.prKafkaEndpoint = settingsDict['prkafkaendpoint']
        self.prKafkaPort = settingsDict['prkafkaport']
        self.prKafkaTopic = settingsDict['prkafkatopic']
        self.grafana_url = settingsDict['grafanaurl']
        self.grafana_credentials = settingsDict['grafanatoken']
        self.grafana_tag = settingsDict['grafanatag']
        self.EDEPort = settingsDict['EDEPort']
        self.index = settingsDict['index']
        self.tfrom = settingsDict['from']
        self.to = settingsDict['to']
        self.query = settingsDict['query']
        self.qsize = settingsDict['qsize']
        self.local = settingsDict['local']
        self.target = settingsDict['target']
        self.augmentations = settingsDict['augmentation']
        self.detectionscaler = settingsDict['detectionscaler']
        self.hpomethod = settingsDict['hpomethod']
        self.hpoparam = settingsDict['hpoparam']
        self.tpot = settingsDict['tpot']
        self.ParamDistribution = settingsDict['ParamDistribution']
        self.nodes = nodesParse(settingsDict['nodes'])
        self.qinterval = settingsDict['qinterval']
        self.categorical = settingsDict['categorical']
        self.train = settingsDict['train']
        self.type = settingsDict['Type']
        self.traintype = settingsDict['traintype']
        self.validationtype = settingsDict['validationtype']
        self.detecttype = settingsDict['detecttype']
        self.load = settingsDict['load']
        self.method = settingsDict['method']
        self.trainmethod = settingsDict['trainMethod']
        self.detectmethod = settingsDict['detectMethod']
        self.cv = settingsDict['cv']
        self.scorers = settingsDict['scorer']
        self.verbosecv = settingsDict['verbosecv']
        self.trainscore = settingsDict['trainscore']
        self.returnestimators = settingsDict['returnestimators']
        self.analysis = settingsDict['analysis']
        self.validate = settingsDict['validate']
        self.learningcurve = settingsDict['LearningCurve']
        self.validationcurve = settingsDict['ValidationCurve']
        self.prc = settingsDict["PrecisionRecallCurve"]
        self.rocauc = settingsDict["ROCAUC"]
        self.rfe = settingsDict["RFE"]
        self.dboundary = settingsDict["DecisionBoundary"]
        self.pred_analysis = settingsDict['PredAnalysis']
        self.export = settingsDict['export']
        self.detect = settingsDict['detect']
        self.sload = settingsDict['sload']
        self.smemory = settingsDict['smemory']
        self.snetwork = settingsDict['snetwork']
        self.methodSettings = settingsDict['MethodSettings']
        self.resetIndex = settingsDict['resetindex']
        self.trainingSet = settingsDict['training']
        self.validationSet = settingsDict['validation']
        self.anoOnly = settingsDict['anomalyOnly']
        self.validratio = settingsDict['validratio']
        self.compare = settingsDict['compare']
        self.dataDir = dataDir
        self.modelsDir = modelsDir
        self.queryDir = queryDir
        self.anomalyIndex = "anomalies"
        self.regnodeList = []
        self.allowedMethodsClustering = ['skm', 'em', 'dbscan', 'sdbscan', 'isoforest']
        self.allowefMethodsClassification = ['randomforest', 'decisiontree', 'sneural', 'adaboost', 'naivebayes', 'rbad']  # TODO
        self.heap = settingsDict['heap']
        self.edeConnector = Connector(esEndpoint=self.esendpoint,
                                      prEndpoint=self.prendoint,
                                      MInstancePort=self.MPort,
                                      dmonPort=self.EDEPort,
                                      index=self.index,
                                      prKafkaEndpoint=self.prKafkaEndpoint,
                                      prKafkaPort=self.prKafkaPort,
                                      prKafkaTopic=self.prKafkaTopic
                                      )
        self.qConstructor = QueryConstructor(self.queryDir)
        self.dformat = DataFormatter(self.dataDir)
        self.cfilter = settingsDict['cfilter']
        self.rfilter = settingsDict['rfilter']
        self.dfilter = settingsDict['dfilter']
        self.fillnan = settingsDict['fillna']
        self.filterlow = settingsDict['filterlow']
        self.dropnan = settingsDict['dropna']
        self.filterwild = settingsDict['filterwild']
        self.checkpoint = settingsDict['checkpoint']
        self.interval = settingsDict['interval']
        self.delay = settingsDict['delay']
        self.point = settingsDict['point']
        self.desiredNodesList = []
        self.sparkReturn = 0
        self.stormReturn = 0
        self.cassandraReturn = 0
        self.mongoReturn = 0
        self.yarnReturn = 0
        self.systemReturn = 0
        self.mapmetrics = 0
        self.reducemetrics = 0
        self.mrapp = 0
        self.userQueryReturn = 0
        self.cepQueryReturn = 0
        self.dataNodeTraining = 0
        self.dataNodeDetecting = 0

    def initConnector(self):
        if self.esendpoint is not None:
            logger.info('[{}] : [INFO] Establishing connection to DMon ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            resdmonInfo = self.edeConnector.getDmonStatus()
            logger.info('[{}] : [INFO] Connection established, status {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), resdmonInfo))

            resInfo = self.edeConnector.info()
            logger.info('[{}] : [INFO]General es dmon info: {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), resInfo))

            interval = self.edeConnector.getInterval()
            try:
                if int(self.qinterval[:-1]) < interval['System']:
                    logger.warning('[{}] : [WARN] System Interval smaller than set interval!, DMon interval is {} while EDE is {}!'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format), self.qinterval, interval['System']))
                else:
                    logger.info('[%s] : [INFO] Query interval check passed!',
                                   datetime.fromtimestamp(time.time()).strftime(log_format))
            except Exception:
                logger.error('[%s] : [ERROR] System Interval not set in dmon!',
                                   datetime.fromtimestamp(time.time()).strftime(log_format))
                sys.exit(1)

            resClusterState = self.edeConnector.clusterHealth()
            logger.info('[{}] : [INFO] ES Backend cluster health: {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), resClusterState))


            logger.info('[{}] : [INFO] Checking dmon registered nodes....'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            self.regnodeList = self.edeConnector.getNodeList()
            logger.info('[{}] : [INFO] Nodes found: {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), self.regnodeList))
            self.desiredNodesList = self.getDesiredNodes()
            if str2Bool(self.resetIndex):
                logger.warning('[{}] : [WARN] Resseting index: {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), self.anomalyIndex))
                self.edeConnector.deleteIndex(self.anomalyIndex)
                logger.warning('[%s] : [WARN] Reset index %s complete',
                               datetime.fromtimestamp(time.time()).strftime(log_format), self.anomalyIndex)
        elif self.prendoint is not None:
            logger.info('[{}] : [INFO] Checking connection to PR Backend ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            self.edeConnector.pr_health_check()
            logger.info('[{}] : [INFO] Fetching PR Backend targets ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            targets = self.edeConnector.pr_targets()
            target_endpoints = []
            for target in targets['data']['activeTargets']:
                target_endpoints.append(target['labels']['instance'])
            logger.info('[{}] : [INFO] Listing PR Backend target endpoints: {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), target_endpoints))
            if targets['data']['droppedTargets']:
                logger.warning('[{}] : [WARNING] Detected PR dropped targets: {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format),
                    targets['data']['droppedTargets'])) # TODO parse droped targets instead of dumping json to log
        elif self.sr_pmds_end is not None:
            if not self.sr_pmds_check:
                logger.info('[{}] : [INFO] Checking connection to Serrano PMDS Backend ...'.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')))
                deployments = self.edeConnector.sr_pmds_service_query_deployments(cluster_uuid=self.sr_cluster_id,
                                                                                  namespace=self.sr_pmds_namespace,
                                                                                  start='-1m',
                                                                                  format='raw'
                                                                              )
                if deployments.status_code != 200:
                    logger.warning('[{}] : [WARN] Serrano PMDS Backend connection returned non-standard status code: {}'.format(
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), deployments.status_code))
        elif self.local is not None:
            logger.info('[{}] : [INFO] Set local data source: {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), self.local))
        else:
            logger.error('[{}] : [ERROR] No valid datasource defined'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            sys.exit(1)

    def getDesiredNodes(self):
        desNodes = []
        if not self.nodes:
            desNodes = self.edeConnector.getNodeList()
            logger.info('[%s] : [INFO] Metrics from all nodes will be collected ',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
        else:
            if set(self.nodes).issubset(set(self.regnodeList)):
                desNodes = self.nodes
                logger.info('[%s] : [INFO] Metrics from %s nodes will be collected ',
                            datetime.fromtimestamp(time.time()).strftime(log_format), str(desNodes))
            else:
                logger.error('[%s] : [ERROR] Registred nodes %s do not contain desired nodes %s ',
                             datetime.fromtimestamp(time.time()).strftime(log_format), str(self.regnodeList),
                             str(desNodes))
                sys.exit(1)
        return desNodes

    def getCategoricalFeatures(self):
        if not self.categorical:
            col = None
        else:
            col = cfilterparse(self.categorical)
        return col

    def getDataPR(self,
                  detect=False):
        if self.local is not None and not detect:
            if checkFile(self.local):
                df_qpr = self.edeConnector.localData(self.local)
                logger.info('[{}] : [INFO] Loading local training file {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), self.local))
            else:
                logger.error('[{}] : [ERROR] Failed to find  local training file {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), self.local))
                sys.exit(1)
                
        elif self.sr_pmds_end is not None:
            checkpoint = str2Bool(self.checkpoint)
            query_param = {
                'cluster_uuid': self.sr_cluster_id,
                'groups': self.sr_pmds_group,
                'start': self.sr_pmds_start,
                'stop': self.sr_pmds_end,
                'format': 'raw',
            }
            r_pmds = self.edeConnector.sr_pmds_query(query_param)
            logger.info('[{}] : [INFO] Fetching data from Serrano PMDS backend with query: {}'.format(
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), query_param))

            df_qpr = self.dformat.sr_pmds_list_to_df(r_pmds, checkpoint=checkpoint, detect=detect)
        else:
            queryd = self.query
            checkpoint = str2Bool(self.checkpoint)
            if queryd is None:
                logger.info('[%s] : [INFO] Query string not defined using default',
                            datetime.fromtimestamp(time.time()).strftime(log_format))
                if self.qinterval:
                    qtime = self.qinterval
                else:
                    qtime = '10m'
                queryd = self.qConstructor.pr_query_node(time=qtime)

            logger.info('[{}] : [INFO] Fetching data from PR backend with query: {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), queryd))
            qpr = self.edeConnector.pr_query(queryd)
            df_qpr = self.dformat.prtoDF(data=qpr, checkpoint=checkpoint, verbose=True, detect=detect)
        return df_qpr

    def getData(self, detect=False):
        if detect:
            tfrom = "now-%s" %self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        queryd = queryParser(self.query)
        checkpoint = str2Bool(self.checkpoint)
        desNodes = self.desiredNodesList
        logger.info('[%s] : [INFO] Checking node list',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        if 'system' in queryd:
            if queryd['system'] == 0:
                logger.info('[{}] : [INFO] Starting query for system metrics ...'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                lload = []
                lmemory = []
                linterface = []
                lpack = []

                for node in desNodes:
                    load, load_file = self.qConstructor.loadString(node)
                    memory, memory_file = self.qConstructor.memoryString(node)
                    interface, interface_file = self.qConstructor.interfaceString(node)
                    packet, packet_file = self.qConstructor.packetString(node)

                    # Queries
                    qload = self.qConstructor.systemLoadQuery(load, tfrom, to, self.qsize, self.qinterval)
                    qmemory = self.qConstructor.systemMemoryQuery(memory, tfrom, to, self.qsize, self.qinterval)
                    qinterface = self.qConstructor.systemInterfaceQuery(interface, tfrom, to, self.qsize, self.qinterval)
                    qpacket = self.qConstructor.systemInterfaceQuery(packet, tfrom, to, self.qsize, self.qinterval)

                    # Execute query and convert response to csv
                    qloadResponse = self.edeConnector.aggQuery(qload)
                    gmemoryResponse = self.edeConnector.aggQuery(qmemory)
                    ginterfaceResponse = self.edeConnector.aggQuery(qinterface)
                    gpacketResponse = self.edeConnector.aggQuery(qpacket)

                    if not checkpoint:
                        self.dformat.dict2csv(ginterfaceResponse, qinterface, interface_file)
                        self.dformat.dict2csv(gmemoryResponse, qmemory, memory_file)
                        self.dformat.dict2csv(qloadResponse, qload, load_file)
                        self.dformat.dict2csv(gpacketResponse, qpacket, packet_file)
                    else:
                        linterface.append(self.dformat.dict2csv(ginterfaceResponse, qinterface, interface_file, df=checkpoint))
                        lmemory.append(self.dformat.dict2csv(gmemoryResponse, qmemory, memory_file, df=checkpoint))
                        lload.append(self.dformat.dict2csv(qloadResponse, qload, load_file, df=checkpoint))
                        lpack.append(self.dformat.dict2csv(gpacketResponse, qpacket, packet_file, df=checkpoint))

                # Merge and rename by node system Files
                logger.info('[{}] : [INFO] Query complete starting merge ...'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                if not checkpoint:
                    self.dformat.chainMergeSystem()
                    # Merge system metricsall
                    merged_df = self.dformat.chainMergeNR()
                    self.dformat.df2csv(merged_df, os.path.join(self.dataDir, "System.csv"))
                    self.systemReturn = 0
                else:
                    df_interface, df_load, df_memory, df_packet = self.dformat.chainMergeSystem(linterface=linterface,
                                                                                                lload=lload, lmemory=lmemory, lpack=lpack)
                    merged_df = self.dformat.chainMergeNR(interface=df_interface, memory=df_memory,
                                                          load=df_load, packets=df_packet)
                    self.systemReturn = merged_df
                logger.info('[{}] : [INFO] System metrics merge complete'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
            else:
                logger.error('[{}] : [ERROR] Only for all system metrics available'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                sys.exit(1)
        if 'yarn' in queryd:
            logger.info('[{}] : [INFO] Starting qurey for yarn metrics ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            if queryd['yarn'] == 0:
                # per slave unique process name list
                nodeProcessReduce = {}
                nodeProcessMap = {}
                # list of dataframes
                lNM = []
                lNMJvm = []
                lShuffle = []
                lDataNode = []
                lmap = {}
                lreduce = {}
                for node in desNodes:
                    nodeManager, nodeManager_file = self.qConstructor.nodeManagerString(node)
                    jvmNodeManager, jvmNodeManager_file = self.qConstructor.jvmnodeManagerString(node)
                    datanode, datanode_file = self.qConstructor.datanodeString(node)
                    shuffle, shuffle_file = self.qConstructor.shuffleString(node)
                    reduce = self.qConstructor.jvmRedProcessString(node)
                    map = self.qConstructor.jvmMapProcessingString(node)

                    qnodeManager = self.qConstructor.yarnNodeManager(nodeManager, tfrom, to, self.qsize, self.qinterval)
                    qjvmNodeManager = self.qConstructor.jvmNNquery(jvmNodeManager, tfrom, to, self.qsize, self.qinterval)
                    qdatanode = self.qConstructor.datanodeMetricsQuery(datanode, tfrom, to, self.qsize, self.qinterval)
                    qshuffle = self.qConstructor.shuffleQuery(shuffle, tfrom, to, self.qsize, self.qinterval)
                    qreduce = self.qConstructor.queryByProcess(reduce, tfrom, to, 500, self.qinterval)
                    qmap = self.qConstructor.queryByProcess(map, tfrom, to, 500, self.qinterval)

                    gnodeManagerResponse = self.edeConnector.aggQuery(qnodeManager)
                    gjvmNodeManagerResponse = self.edeConnector.aggQuery(qjvmNodeManager)
                    gshuffleResponse = self.edeConnector.aggQuery(qshuffle)
                    gdatanode = self.edeConnector.aggQuery(qdatanode)
                    greduce = self.edeConnector.aggQuery(qreduce)
                    gmap = self.edeConnector.aggQuery(qmap)

                    if list(gnodeManagerResponse['aggregations'].values())[0].values()[0]:
                        if not checkpoint:
                            self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file)
                        else:
                            lNM.append(self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file, df=checkpoint))
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                                    datetime.fromtimestamp(time.time()).strftime(log_format), node)

                    if list(gjvmNodeManagerResponse['aggregations'].values())[0].values()[0]:
                        if not checkpoint:
                            self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file)
                        else:
                            lNMJvm.append(self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file, df=checkpoint))
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                                    datetime.fromtimestamp(time.time()).strftime(log_format), node)

                    if list(gshuffleResponse['aggregations'].values())[0].values()[0]:
                        if not checkpoint:
                            self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file)
                        else:
                            lShuffle.append(self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file, df=checkpoint))
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no shuffle metrics!',
                                    datetime.fromtimestamp(time.time()).strftime(log_format), node)

                    if list(gdatanode['aggregations'].values())[0].values()[0]:
                        if not checkpoint:
                            self.dformat.dict2csv(gdatanode, qdatanode, datanode_file)
                        else:
                            lDataNode.append(self.dformat.dict2csv(gdatanode, qdatanode, datanode_file, df=checkpoint))
                    else:
                        logger.info('[%s] : [INFO] Empty response from  %s no datanode metrics!',
                                    datetime.fromtimestamp(time.time()).strftime(log_format), node)

                    uniqueReduce = set()
                    for i in greduce['hits']['hits']:
                        # print i['_source']['ProcessName']
                        uniqueReduce.add(i['_source']['ProcessName'])
                    nodeProcessReduce[node] = list(uniqueReduce)

                    uniqueMap = set()
                    for i in gmap['hits']['hits']:
                        # print i['_source']['ProcessName']
                        uniqueMap.add(i['_source']['ProcessName'])
                    nodeProcessMap[node] = list(uniqueMap)
                # Get Process info by host and name
                for host, processes in nodeProcessReduce.items():
                    if processes:
                        for process in processes:
                            logger.info('[%s] : [INFO] Reduce process %s for host  %s found',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), process,
                                            host)
                            hreduce, hreduce_file = self.qConstructor.jvmRedProcessbyNameString(host, process)
                            qhreduce = self.qConstructor.jvmNNquery(hreduce, tfrom, to, self.qsize, self.qinterval)
                            ghreduce = self.edeConnector.aggQuery(qhreduce)
                            if not checkpoint:
                                self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file)
                            else:
                                lreduce[process] = self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file, df=checkpoint)
                    else:
                        logger.info('[%s] : [INFO] No reduce process for host  %s found',
                                        datetime.fromtimestamp(time.time()).strftime(log_format), host)
                        pass

                for host, processes in nodeProcessMap.items():
                    if processes:
                        for process in processes:
                            logger.info('[%s] : [INFO] Map process %s for host  %s found',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), process,
                                            host)
                            hmap, hmap_file = self.qConstructor.jvmMapProcessbyNameString(host, process)
                            qhmap = self.qConstructor.jvmNNquery(hmap, tfrom, to, self.qsize, self.qinterval)
                            ghmap = self.edeConnector.aggQuery(qhmap)
                            if not checkpoint:
                                self.dformat.dict2csv(ghmap, qhmap, hmap_file)
                            else:
                                lmap[process] = self.dformat.dict2csv(ghmap, qhmap, hmap_file, df=checkpoint)
                    else:
                        logger.info('[%s] : [INFO] No map process for host  %s found',
                                        datetime.fromtimestamp(time.time()).strftime(log_format), host)
                        pass

                        # Get non host based metrics queries and file strings
                dfs, dfs_file = self.qConstructor.dfsString()
                dfsFs, dfsFs_file = self.qConstructor.dfsFString()
                jvmNameNodeString, jvmNameNode_file = self.qConstructor.jvmNameNodeString()
                queue, queue_file = self.qConstructor.queueResourceString()
                cluster, cluster_file = self.qConstructor.clusterMetricsSring()
                jvmResMng, jvmResMng_file = self.qConstructor.jvmResourceManagerString()
                mrapp, mrapp_file = self.qConstructor.mrappmasterString()  #todo
                jvmMrapp, jvmMrapp_file = self.qConstructor.jvmMrappmasterString()
                fsop, fsop_file = self.qConstructor.fsopDurationsString()

                # Queries
                qdfs = self.qConstructor.dfsQuery(dfs, tfrom, to, self.qsize, self.qinterval)
                qdfsFs = self.qConstructor.dfsFSQuery(dfsFs, tfrom, to, self.qsize, self.qinterval)
                qjvmNameNode = self.qConstructor.jvmNNquery(jvmNameNodeString, tfrom, to, self.qsize, self.qinterval)
                qqueue = self.qConstructor.resourceQueueQuery(queue, tfrom, to, self.qsize, self.qinterval)
                qcluster = self.qConstructor.clusterMetricsQuery(cluster, tfrom, to, self.qsize, self.qinterval)
                qjvmResMng = self.qConstructor.jvmNNquery(jvmResMng, tfrom, to, self.qsize, self.qinterval)
                qjvmMrapp = self.qConstructor.jvmNNquery(jvmMrapp, tfrom, to, self.qsize, self.qinterval)
                qfsop = self.qConstructor.fsopDurationsQuery(fsop, tfrom, to, self.qsize, self.qinterval)


                # Responses
                gdfs = self.edeConnector.aggQuery(qdfs)
                gdfsFs = self.edeConnector.aggQuery(qdfsFs)
                gjvmNameNode = self.edeConnector.aggQuery(qjvmNameNode)
                gqueue = self.edeConnector.aggQuery(qqueue)
                gcluster = self.edeConnector.aggQuery(qcluster)
                gjvmResourceManager = self.edeConnector.aggQuery(qjvmResMng)
                gjvmMrapp = self.edeConnector.aggQuery(qjvmMrapp)
                gfsop = self.edeConnector.aggQuery(qfsop)

                if not checkpoint:
                    self.dformat.dict2csv(gdfs, qdfs, dfs_file)
                    self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file)
                    self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file)
                    self.dformat.dict2csv(gqueue, qqueue, queue_file)
                    self.dformat.dict2csv(gcluster, qcluster, cluster_file)
                    self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file)
                    self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file)
                    self.dformat.dict2csv(gfsop, qfsop, fsop_file)

                    logger.info('[{}] : [INFO] Query for yarn metrics complete starting merge ...'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format)))
                    merged_DFS = self.dformat.chainMergeDFS()
                    self.dformat.df2csv(merged_DFS, os.path.join(self.dataDir, 'DFS_Merged.csv'))

                    merged_cluster = self.dformat.chainMergeCluster()
                    self.dformat.df2csv(merged_cluster, os.path.join(self.dataDir, 'Cluster_Merged.csv'))

                    nm_merged, jvmnn_merged, nm_Shuffle = self.dformat.chainMergeNM()
                    self.dformat.df2csv(nm_merged, os.path.join(self.dataDir, 'NM_Merged.csv'))
                    self.dformat.df2csv(jvmnn_merged, os.path.join(self.dataDir, 'JVM_NM_Merged.csv'))
                    self.dformat.df2csv(nm_Shuffle, os.path.join(self.dataDir, 'NM_Shuffle.csv'))

                    dn_merged = self.dformat.chainMergeDN()
                    self.dformat.df2csv(dn_merged, os.path.join(self.dataDir, 'DN_Merged.csv'))

                    final_merge = self.dformat.mergeFinal()
                    self.dformat.df2csv(final_merge, os.path.join(self.dataDir, 'Final_Merge.csv'))
                    logger.info('[%s] : [INFO] Yarn metrics merge complete',
                                datetime.fromtimestamp(time.time()).strftime(log_format))
                    self.yarnReturn = 0
                else:
                   df_dfs = self.dformat.dict2csv(gdfs, qdfs, dfs_file, df=checkpoint)
                   df_dfsFs = self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file, df=checkpoint)
                   df_queue = self.dformat.dict2csv(gqueue, qqueue, queue_file, df=checkpoint)
                   df_cluster = self.dformat.dict2csv(gcluster, qcluster, cluster_file, df=checkpoint)
                   df_jvmResourceManager = self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file, df=checkpoint)
                   df_jvmMrapp = self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file, df=checkpoint)
                   df_fsop = self.dformat.dict2csv(gfsop, qfsop, fsop_file, df=checkpoint)

                   merged_DFS = self.dformat.chainMergeDFS(dfs=df_dfs, dfsfs=df_dfsFs, fsop=df_fsop)
                   merged_cluster = self.dformat.chainMergeCluster(clusterMetrics=df_cluster, queue=df_queue,
                                                                   jvmRM=df_jvmResourceManager)
                   merge_nodemanager, jvmNode_manager, mShuffle= self.dformat.chainMergeNM(lNM=lNM, lNMJvm=lNMJvm, lShuffle=lShuffle)
                   datanode_merge = self.dformat.chainMergeDN(lDN=lDataNode)
                   df_jvmNameNode = self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file, df=checkpoint)
                   final_merge = self.dformat.mergeFinal(dfs=merged_DFS, cluster=merged_cluster, nodeMng=merge_nodemanager,
                                                         jvmnodeMng=jvmNode_manager, dataNode=datanode_merge,
                                                         jvmNameNode=df_jvmNameNode, shuffle=mShuffle,
                                                         system=self.systemReturn)

                   self.yarnReturn = final_merge
                logger.info('[%s] : [INFO] Yarn metrics merge complete',
                            datetime.fromtimestamp(time.time()).strftime(log_format))
                self.yarnReturn = final_merge
                self.mapmetrics = lmap
                self.reducemetrics = lreduce
                self.mrapp = df_jvmMrapp
            else:
                # cluster, nn, nm, dfs, dn, mr
                mCluster = mNameNode = mNodeManager = mNodeManagerJVM = mShuffle = mDFS = mDataNode = mMap = mReduce = 0
                for el in queryd['yarn']:
                    if el == 'cluster':
                        mCluster = self.getCluster(detect=detect)
                    if el == 'nn':
                        mNameNode = self.getNameNode(detect=detect)
                    if el == 'nm':
                        mNodeManager, mNodeManagerJVM, mShuffle = self.getNodeManager(desNodes, detect=detect)
                    if el == 'dfs':
                        mDFS = self.getDFS(detect=detect)
                    if el == 'dn':
                        mDataNode = self.getDataNode(desNodes, detect=detect)
                    if el == 'mr':
                       mMap, mReduce, mMRApp = self.getMapnReduce(desNodes, detect=detect)
                    if el not in ['cluster', 'nn', 'nm', 'dfs', 'dn', 'mr']:
                        logger.error('[%s] : [ERROR] Unknown metrics context %s',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), el)
                        sys.exit(1)
            if not checkpoint:
                final_merge = self.dformat.mergeFinal()
                self.dformat.df2csv(final_merge, os.path.join(self.dataDir, 'Final_Merge.csv'))
                self.yarnReturn = 0
            else:
                final_merge = self.dformat.mergeFinal(dfs=mDFS, cluster=mCluster, nodeMng=mNodeManager,
                                                      jvmnodeMng=mNodeManagerJVM, dataNode=mDataNode,
                                                      jvmNameNode=mNameNode, shuffle=mShuffle, system=self.systemReturn)
                self.yarnReturn = final_merge
                self.reducemetrics = mReduce
                self.mapmetrics = mMap
                self.mrapp = mMRApp
                self.dformat.df2csv(final_merge, os.path.join(self.dataDir, 'cTest.csv'))
            logger.info('[{}] : [INFO] Finished query and merge for yarn metrics'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))

        elif 'spark' in queryd:
            logger.info('[{}] : [INFO] Starting query for Spark metrics'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            self.sparkReturn = self.getSpark(detect=detect)
        elif 'storm' in queryd:
            logger.info('[{}] : [INFO] Starting query for Storm metrics'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            stormTopology = self.edeConnector.getStormTopology()
            try:
                bolts = stormTopology['bolts']
                spouts = stormTopology['spouts']
                topology = stormTopology['Topology']
            except Exception as inst:
                logger.error('[%s] : [ERROR] No Storm topology found with %s',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), str(stormTopology))
                sys.exit(1)
            storm, storm_file = self.qConstructor.stormString()
            qstorm = self.qConstructor.stormQuery(storm, tfrom, to, self.qsize, self.qinterval, bolts=bolts, spouts=spouts)
            gstorm = self.edeConnector.aggQuery(qstorm)
            if not checkpoint:
                self.dformat.dict2csv(gstorm, qstorm, storm_file)
                self.stormReturn = 0
            else:
                df_storm = self.dformat.dict2csv(gstorm, qstorm, storm_file, df=checkpoint)
                self.stormReturn = df_storm
        elif 'cassandra' in queryd:
            # desNodes = ['cassandra-1']  #REMOVE only for casasndra testing
            self.cassandraReturn = self.getCassandra(desNodes, detect=detect)
        elif 'mongodb' in queryd:
            self.mongoReturn = self.getMongodb(desNodes, detect=detect)
        elif 'userquery' in queryd:
            self.userQueryReturn = self.getQuery(detect=detect)
        elif 'cep' in queryd:
            self.cepQueryReturn = self.getCEP(detect=detect)
        return self.systemReturn, self.yarnReturn, self.reducemetrics, self.mapmetrics, self.mrapp, self.sparkReturn, self.stormReturn, self.cassandraReturn, self.mongoReturn, self.userQueryReturn, self.cepQueryReturn

    def filterData(self, df,
                   m=False,
                   df_index='time',
                   detect=False):
        '''
        :param df: -> dataframe to be filtered
        :param m: -> modify df in place or copy
        :return: ->  filtred df
        '''
        checkpoint = str2Bool(self.checkpoint)
        if self.cfilter is None:
            logger.info('[%s] : [INFO] Column filter not set skipping',
                                            datetime.fromtimestamp(time.time()).strftime(log_format))
        else:
            if not cfilterparse(self.cfilter):
                logger.warning('[%s] : [WARN] Column filter is empty skipping',
                                            datetime.fromtimestamp(time.time()).strftime(log_format))
            else:
                logger.info('[%s] : [INFO] Column filter is set to %s filtering ...',
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.cfilter)
                df = self.dformat.filterColumns(df, cfilterparse(self.cfilter))
        if self.rfilter is None:
            logger.info('[%s] : [INFO] Row filter not set skipping',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
        else:
            ld, gd = rfilterparse(self.rfilter)
            if ld == 0 and gd == 0:
                logger.info('[%s] : [INFO] Both ld and gd are set to zero skipping row filter',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
            else:
                logger.info('[%s] : [INFO] Row filter is set to gd->%s and ld->%s filtering',
                            datetime.fromtimestamp(time.time()).strftime(log_format), ld, gd)
                df = self.dformat.filterRows(df, int(ld), int(gd))
        if self.dfilter is None:
            logger.info('[%s] : [INFO] Drop columns not set skipping',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
        else:
            if not cfilterparse(self.dfilter):
                logger.warning('[%s] : [WARN] Drop column filter is empty skipping',
                               datetime.fromtimestamp(time.time()).strftime(log_format))
            else:
                logger.info('[{}] : [INFO] Drop columns set to {}, fitlering ...'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), self.dfilter))
                if m:
                    self.dformat.dropColumns(df, cfilterparse(self.dfilter), cp=False)
                else:
                    df = self.dformat.dropColumns(df, cfilterparse(self.dfilter))
        if self.filterlow:
            self.dformat.filterLowVariance(df)
        if self.fillnan:
            self.dformat.fillMissing(df)
        if self.dropnan:
            self.dformat.dropMissing(df)

        if df_index is not None:
            df.set_index(df_index, inplace=True)

        if self.filterwild:
            if "Regex" not in self.filterwild.keys():
                logger.error('[%s] : [ERROR] Missing parameters for filtering via wildcard exiting ...',
                               datetime.fromtimestamp(time.time()).strftime(log_format))
                sys.exit(1)
            if "Keep" in self.filterwild.keys():
                df = self.dformat.filterWildcard(df, wild_card=self.filterwild['Regex'], keep=self.filterwild['Keep'])
            else:
                df = self.dformat.filterWildcard(df, wild_card=self.filterwild['Regex'])


        if self.categorical is None:
            logger.info('[%s] : [INFO] Skipping categorical feature conversion',
                               datetime.fromtimestamp(time.time()).strftime(log_format))
        else:
            logger.info('[%s] : [INFO] Starting categorical feature conversion',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
            col = self.getCategoricalFeatures()
            df, v, o = self.dformat.ohEncoding(df, cols=col)
        if checkpoint:
            logger.info('[{}] : [INFO] Checkpointing  filtered data ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            if detect:
                pr_f = "pr_data_detect_filtered.csv"
            else:
                pr_f = 'pr_data_filtered.csv'
            df.to_csv(os.path.join(self.dataDir, pr_f))
        logger.info('[{}] : [INFO] Filtered data shape {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), df.shape))
        if df.shape[0] == 0:
            logger.error('[{}] : [ERROR] Empty dataframe rezulted after filtering! Exiting!'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
            sys.exit(1)
        return df

    def trainDaskMethod(self):
        if str2Bool(self.train):
            logger.info('[{}] : [INFO] Training started. Getting data ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            checkpoint = str2Bool(self.checkpoint)
            pr_data = self.getDataPR()
            if self.traintype == 'classification' or self.traintype == 'hpo' or self.traintype =='tpot':
                pr_data, y = self.dformat.getGT(pr_data, gt=self.target)
            udata = self.filterData(pr_data)
            if self.augmentations is not None:
                try:
                    scaler_type = self.augmentations['Scaler']

                except Exception:
                    scaler_type = False

                try:
                    sudata = self.dformat.scale(data=udata, scaler_type=scaler_type)
                except Exception as inst:
                    logger.warning('[{}] : [WARN] Failed to initialize scaler with {} and {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
                    sudata = udata
                try:
                    operations = self.augmentations['Operations']
                    try:
                        remove_filtered = self.augmentations['Operations']['RemoveFiltered']
                    except Exception:
                        remove_filtered = True
                except Exception:
                    operations = False
                    remove_filtered = False

                asudata = self.dformat.computeOnColumns(sudata, operations=operations, remove_filtered=remove_filtered)
                if checkpoint:
                    asudata.to_csv(os.path.join(self.dataDir, 'pr_data_augmented.csv'))
            else:
                asudata = udata
            # User defined analysis
            if self.analysis is not None:
                self.analisysDask(pr_data)
            if self.traintype == 'clustering':
                logger.info('[{}] : [INFO] Training clusterer ...'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                if self.trainmethod == 'isoforest':  # TODO: merge pyod models from private dev repo
                    disofrst = sede.SciCluster(self.modelsDir)
                    isofrstmodel = disofrst.dask_isolationForest(settings=self.methodSettings, mname=self.export, data=asudata)
                elif self.trainmethod == 'sdbscan':
                    dsdbscan = sede.SciCluster(self.modelsDir)
                    dbscanmodel = dsdbscan.dask_sdbscanTrain(settings=self.methodSettings, mname=self.export, data=asudata)
                else:
                    if not isinstance(self.trainmethod, str):
                        logger.info('[{}] : [INFO] Detected user defined method, initializing ...'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format)))
                        umeth = sede.SciCluster(self.modelsDir)
                        umod = umeth.dask_clusterMethod(cluster_method=self.trainmethod, mname=self.export, data=asudata)
                    else:
                        logger.error('[{}] : [ERROR] Unknown Clustering method {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), self.type))
                        sys.exit(1)
                logger.info('[{}] : [INFO] Clustering Complete.'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format)))

            elif self.traintype == 'classification':
                logger.info('[{}] : [INFO] Training classifier ...'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                classede = cede.SciClassification(self.modelsDir, self.dataDir, self.checkpoint, self.export,
                                                  training=self.trainingSet, validation=self.validationSet,
                                                  validratio=self.validratio, compare=self.compare, cv=self.cv,
                                                  trainscore=self.trainscore, scorers=self.scorers,
                                                  returnestimators=self.returnestimators,
                                                  verbose=self.verbosecv, learningcurve=self.learningcurve,
                                                  validationcurve=self.validationcurve, prc=self.prc,
                                                  rocauc=self.rocauc, rfe=self.rfe, dboundary=self.dboundary)
                clf = classede.dask_classifier(settings=self.methodSettings, mname=self.export, X=asudata, y=y,
                                               classification_method=self.trainmethod)

                logger.info('[{}] : [INFO] Classification Complete.'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
            elif self.traintype == 'hpo':
                logger.info('[{}] : [INFO] Stating HPO.'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                classede = cede.SciClassification(self.modelsDir, self.dataDir, self.checkpoint, self.export,
                                                  training=self.trainingSet, validation=self.validationSet,
                                                  validratio=self.validratio, compare=self.compare, cv=self.cv,
                                                  trainscore=self.trainscore, scorers=self.scorers,
                                                  returnestimators=self.returnestimators)
                clf = classede.dask_hpo(param_dist=self.ParamDistribution,
                                        mname="w",
                                        X=pr_data,
                                        y=y,
                                        hpomethod=self.hpomethod,
                                        hpoparam=self.hpoparam,
                                        classification_method=self.trainmethod)
                logger.info('[{}] : [INFO] HPO Completed.'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
            elif self.traintype == 'tpot':
                classede = cede.SciClassification(self.modelsDir, self.dataDir, self.checkpoint, self.export,
                                                  training=self.trainingSet, validation=self.validationSet,
                                                  validratio=self.validratio, compare=self.compare, cv=self.cv,
                                                  trainscore=self.trainscore, scorers=self.scorers,
                                                  returnestimators=self.returnestimators)
                clf = classede.dask_tpot(self.tpot,
                                         X=pr_data,
                                         y=y)
            else:
                logger.error('[{}] : [ERROR] Unknown training type {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), self.type))
                sys.exit(1)
            logger.info('[{}] : [INFO] Training complete'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
        else:
            logger.warning('[%s] : [WARN] Training is set to false, skipping...',
                           datetime.fromtimestamp(time.time()).strftime(log_format))
            return 0

    def analisysDask(self, data):
        if self.analysis is None:
            pass
        else:
            if len(self.analysis['Methods']) == 0:
                logger.warn('[{}] : [WARN] No analysis methods have been defined'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                pass
            else:
                logger.info('[{}] : [INFO] Starting user defined analysis'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                for manalysis in self.analysis['Methods']:
                    self.__analysisMethod(manalysis['Method'], data)
                if str2Bool(self.analysis['Solo']):
                    logger.warning('[{}] : [WARN] Only analysis set to run, skipping other tasks!'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format)))
                    logger.info('[{}] : [INFO] Exiting EDE framework'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format)))
                    sys.exit(0)
        return 0

    def __analysisMethod(self, method,
                          data):
        try:
            logger.info('[{}] : [INFO] Loading user defined analysis: {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), method.__name__))
            data_op = method(data)
        except Exception as inst:
            logger.error('[{}] : [ERROR] Failed to load user analysis {} with {} and {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), method.__name__, type(inst), inst.args))
            return data
        logger.info('[{}] : [INFO] Finished user analysis: {}'.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), method.__name__))
        return data_op

    def trainMethodSelector(self):
        if str2Bool(self.train):
            logger.info('[{}] : [INFO] Getting data ...'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
            checkpoint = str2Bool(self.checkpoint)
            queryd = queryParser(self.query)
            systemReturn, yarnReturn, reducemetrics, mapmetrics, mrapp, sparkReturn, stormReturn, cassandraReturn, mongoReturn, userqueryReturn, cepQueryReturn = self.getData()
            if not checkpoint:
                if 'yarn' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Final_Merge.csv'))
                elif 'storm' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Storm.csv'))
                elif 'cassandra' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Merged_Cassandra.csv'))
                elif 'mongodb' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Merged_Mongo.csv'))
                elif 'spark' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'Spark.csv'))
                elif 'userquery' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'query_response.csv'))
                elif 'cep' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'CEP.csv'))
                elif 'system' in queryd:
                    udata = self.dformat.toDF(os.path.join(self.dataDir, 'System.csv'))
            else:
                if 'yarn' in queryd:
                    udata = yarnReturn
                elif 'storm' in queryd:
                    udata = stormReturn #todo important implement storm, spark, cassandra and mongodb switching
                elif 'cassandra' in queryd:
                    udata = cassandraReturn
                elif 'mongodb' in queryd:
                    udata = mongoReturn
                elif 'spark' in queryd:
                    udata = sparkReturn
                elif 'userquery' in queryd:
                    udata = userqueryReturn
                elif 'cep' in queryd:
                    udata = cepQueryReturn
                elif 'system' in queryd:
                    udata = systemReturn
            udata = self.filterData(udata)  # todo check
            if self.type == 'clustering':
                if self.method in self.allowedMethodsClustering:
                    logger.info('[{}] : [INFO] Training with selected method {} of type {}'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format), self.method, self.type))
                    if checkpoint:
                        dfcomp = ['sdbscan', 'isoforest']  # TODO expand to all dataframe supporting methods
                        if self.method not in dfcomp:
                            dataf = tempfile.NamedTemporaryFile(suffix='.csv')
                            self.dformat.df2csv(udata, dataf.name)
                            data = dataf.name
                    else:
                        if 'yarn' in queryd:
                            data = os.path.join(self.dataDir, 'Final_Merge.csv')
                            if not os.path.isfile(data):
                                logger.error('[%s] : [ERROR] File %s does not exist, cannot load data! Exiting ...',
                                             datetime.fromtimestamp(time.time()).strftime(log_format),
                                             str(data))
                                sys.exit(1)
                        elif 'storm' in queryd:
                            data = os.path.join(self.dataDir, 'Storm.csv')
                        elif 'cassandra' in queryd:
                            data = os.path.join(self.dataDir, 'Merged_Cassandra.csv')
                        elif 'mongodb' in queryd:
                            data = os.path.join(self.dataDir, 'Merged_Mongo.csv')
                        elif 'userquery' in queryd:
                            data = os.path.join(self.dataDir, 'query_response.csv')
                        elif 'cep' in queryd:
                            data = os.path.join(self.dataDir, 'cep.csv')
                        # data = dataf
                    if self.method == 'skm':
                        logger.info('[{}] : [INFO] Method {} settings detected: {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.method, self.methodSettings))
                        opt = parseMethodSettings(self.methodSettings)
                        if not opt:
                            opt = ['-S', '10', '-N', '10']
                        try:
                            raise Exception("Weka is no longer supported!!")
                        except Exception as inst:
                            logger.error('[%s] : [ERROR] Unable to run training for method %s exited with %s and %s',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), self.method, type(inst), inst.args)
                            sys.exit(1)
                    elif self.method == 'em':
                        logger.info('[{}] : [INFO] Method {} settings detected: {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.method, str(self.methodSettings)))
                        opt = parseMethodSettings(self.methodSettings)
                        if not opt:
                            opt = ["-I", "1000", "-N", "6",  "-M", "1.0E-6", "-num-slots", "1", "-S", "100"]
                        try:
                            raise Exception("Weka is no longer supported!!")
                        except Exception as inst:
                            logger.error('[%s] : [ERROR] Unable to run training for method %s exited with %s and %s',
                                         datetime.fromtimestamp(time.time()).strftime(log_format), self.method,
                                         type(inst), inst.args)
                            sys.exit(1)
                    elif self.method == 'dbscan':
                        logger.info('[{}] : [INFO] Method {} settings detected: {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.method,
                            self.methodSettings))
                        opt = parseMethodSettings(self.methodSettings)
                        if not opt:
                            opt = ["-E",  "0.9",  "-M", "6",  "-D", "weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject"]
                        try:
                            # self.dweka.dbscanTrain(dataf=data, options=opt, mname=self.export)
                            raise Exception("Weka is no longer supported!!")
                        except Exception as inst:
                            logger.error('[%s] : [ERROR] Unable to run training for method %s exited with %s and %s',
                                         datetime.fromtimestamp(time.time()).strftime(log_format), self.method,
                                         type(inst), inst.args)
                            sys.exit(1)
                    elif self.method == 'sdbscan':
                        opt = self.methodSettings
                        if not opt or 'leaf_size' not in opt:
                            opt = {'eps': 0.9, 'min_samples': 10, 'metric': 'euclidean',
                                   'algorithm': 'auto', 'leaf_size': 30, 'p': 0.2, 'n_jobs': 1}
                        logger.info('[%s] : [INFO] Using settings for sdbscan -> %s ',
                                    datetime.fromtimestamp(time.time()).strftime(log_format), str(opt))
                        db = sede.SciCluster(self.modelsDir)
                        dbmodel = db.sdbscanTrain(settings=opt, mname=self.export, data=udata)
                    elif self.method == 'isoforest':
                        opt = self.methodSettings
                        if not opt or 'contamination' not in opt:
                            opt = {'n_estimators': 100, 'max_samples': 100, 'contamination': 0.01, 'bootstrap': False,
                                   'max_features': 1.0, 'n_jobs': -1, 'random_state': None, 'verbose': 0}
                        logger.info('[%s] : [INFO] Using settings for isoForest -> %s ',
                                    datetime.fromtimestamp(time.time()).strftime(log_format), str(opt))
                        isofrst = sede.SciCluster(self.modelsDir)
                        isofrstmodel = isofrst.isolationForest(settings=opt, mname=self.export, data=udata)
                    # Once training finished set training to false
                    self.train = False
                    return self.modelName(self.method, self.export)
                else:
                    logger.error('[%s] : [ERROR] Unknown method %s of type %s ',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), self.method, self.type)
                    sys.exit(1)
            elif self.type == 'classification':
                classdmon = cede.SciClassification(self.modelsDir, self.dataDir, self.checkpoint, self.export,
                                                   training=self.trainingSet, validation=self.validationSet,
                                                   validratio=self.validratio, compare=self.compare)
                if self.method in self.allowefMethodsClassification:
                    if self.trainingSet is None:
                        logger.info('[%s] : [INFO] Started Training Set generation ... ',
                                     datetime.fromtimestamp(time.time()).strftime(log_format))
                        udata = classdmon.trainingDataGen(self.methodSettings, udata, onlyAno=self.anoOnly)

                    if self.method == 'randomforest':
                        logger.info('[%s] : [INFO] Initializaing RandomForest model creation ....',
                                    datetime.fromtimestamp(time.time()).strftime(log_format))
                        rfmodel = classdmon.randomForest(settings=self.methodSettings, data=udata, dropna=True)
                    elif self.method == 'decisiontree':
                        logger.info('[%s] : [INFO] Initializaing Decision Tree model creation ....',
                                    datetime.fromtimestamp(time.time()).strftime(log_format))
                        dtmodel = classdmon.decisionTree(settings=self.methodSettings, data=udata, dropna=True)
                    elif self.method == 'sneural':
                        logger.info('[%s] : [INFO] Initializaing Neural Network model creation ....',
                                    datetime.fromtimestamp(time.time()).strftime(log_format))
                        nnmodel = classdmon.neuralNet(settings=self.methodSettings, data=udata, dropna=True)
                    elif self.method == 'adaboost':
                        logger.info('[%s] : [INFO] Initializaing Ada Boost model creation ....',
                                    datetime.fromtimestamp(time.time()).strftime(log_format))
                        admodel = classdmon.adaBoost(settings=self.methodSettings, data=udata, dropna=True)
                    elif self.method == 'naivebayes':
                        print('NaiveBayes not available in this version!')
                        logger.warning('[%s] : [WARN] NaiveBayes not available in this version!',
                                    datetime.fromtimestamp(time.time()).strftime(log_format))
                        sys.exit(0)
                    elif self.method == 'rbad':
                        rbad_home = os.environ['RBAD_HOME'] = os.getenv('RBAD_HOME', os.getcwd())
                        rbad_exec = os.path.join(rbad_home, 'RBAD')

                        if os.path.isfile(rbad_exec):
                            logger.error('[%s] : [ERROR] RBAD Executable nor found at %s',
                                           datetime.fromtimestamp(time.time()).strftime(log_format), rbad_exec)
                            sys.exit(1)
                        rbadPID = 0
                        try:
                            rbadPID = subprocess.Popen(rbad_exec, stdout=subprocess.PIPE,
                                                     close_fds=True).pid
                        except Exception as inst:
                            logger.error("[%s] : [ERROR] Cannot start RBAD with %s and %s",
                                             datetime.fromtimestamp(time.time()).strftime(log_format),
                                             type(inst), inst.args)
                            sys.exit(1)
                        logger.info('[%s] : [WARN] RBAD finished!',
                                    datetime.fromtimestamp(time.time()).strftime(log_format))
                        sys.exit(0)
                    self.train = False
                else:
                    logger.error('[%s] : [ERROR] Unknown method %s of type %s ',
                                 datetime.fromtimestamp(time.time()).strftime(log_format), self.method,
                                 self.type)
                    sys.exit(1)
            else:
                logger.error('[%s] : [ERROR] Unknown type %s ',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), self.type)
                sys.exit(1)
        else:
            logger.warning('[%s] : [WARN] Training is set to false, skipping...',
                                            datetime.fromtimestamp(time.time()).strftime(log_format))
            return 0

    def detectPointAnomalies(self):
        loadth = pointThraesholds(self.sload)
        if not loadth:
            loadth = {'shortterm': {'threashold': '4.5', 'bound': 'gd'},
                      'longterm': {'threashold': '3.0', 'bound': 'gd'}, 'midterm': {'threashold': '3.5', 'bound': 'gd'}}
            logger.warning('[%s] : [WARN] Using default values for point anomaly load',
                           datetime.fromtimestamp(time.time()).strftime(log_format))
        networkth = pointThraesholds(self.snetwork)
        if not loadth:
            networkth = {'rx': {'threashold': '1000000000', 'bound': 'gd'},
                         'tx': {'threashold': '1000000000', 'bound': 'gd'}}
            logger.warning('[%s] : [WARN] Using default values for point anomaly network',
                           datetime.fromtimestamp(time.time()).strftime(log_format))
        memoryth = pointThraesholds(self.smemory)
        if not memoryth:
            memoryth = {'cached': {'threashold': '231313', 'bound': 'gd'},
                        'buffered': {'threashold': '200000000', 'bound': 'gd'},
                        'used': {'threashold': '1000000000', 'bound': 'gd'},
                        'free': {'threashold': '100000000', 'bound': 'ld'}}
            logger.warning('[%s] : [WARN] Using default values for point anomaly memory',
                           datetime.fromtimestamp(time.time()).strftime(log_format))
        all = [loadth, networkth, memoryth]
        while True:
            lload = []
            lmemory = []
            linterface = []
            lpack = []
            tfrom = "now-30s"  # todo compute based on delay
            to = "now"
            for node in self.desiredNodesList:
                load, load_file = self.qConstructor.loadString(node)
                memory, memory_file = self.qConstructor.memoryString(node)
                interface, interface_file = self.qConstructor.interfaceString(node)
                packet, packet_file = self.qConstructor.packetString(node)

                # Queries
                qload = self.qConstructor.systemLoadQuery(load, tfrom, to, self.qsize, self.qinterval)
                qmemory = self.qConstructor.systemMemoryQuery(memory, tfrom, to, self.qsize, self.qinterval)
                qinterface = self.qConstructor.systemInterfaceQuery(interface, tfrom, to, self.qsize,
                                                                    self.qinterval)
                qpacket = self.qConstructor.systemInterfaceQuery(packet, tfrom, to, self.qsize, self.qinterval)

                # Execute query and convert response to csv
                qloadResponse = self.edeConnector.aggQuery(qload)
                gmemoryResponse = self.edeConnector.aggQuery(qmemory)
                ginterfaceResponse = self.edeConnector.aggQuery(qinterface)
                gpacketResponse = self.edeConnector.aggQuery(qpacket)

                linterface.append(self.dformat.dict2csv(ginterfaceResponse, qinterface, interface_file, df=True))
                lmemory.append(self.dformat.dict2csv(gmemoryResponse, qmemory, memory_file, df=True))
                lload.append(self.dformat.dict2csv(qloadResponse, qload, load_file, df=True))
                lpack.append(self.dformat.dict2csv(gpacketResponse, qpacket, packet_file, df=True))

                df_interface, df_load, df_memory, df_packet = self.dformat.chainMergeSystem(linterface=linterface,
                                                                                            lload=lload, lmemory=lmemory,
                                                                                            lpack=lpack)
                df_system = self.dformat.chainMergeNR(interface=df_interface, memory=df_memory,
                                                      load=df_load, packets=df_packet)
                dict_system = self.dformat.df2dict(df_system)
                # print dict_system
                for th in all:
                    for type, val in th.items():
                        responseD = {}
                        if val['bound'] == 'gd':
                            anomalies = self.edepoint.detpoint(dict_system, type=type, threashold=val['threashold'], lt=False)
                            if anomalies:
                                responseD['anomalies'] = anomalies
                                # self.reportAnomaly(responseD)
                            else:
                                logger.info('[%s] : [INFO] No point anomalies detected for type %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type)
                        else:
                            anomalies = self.edepoint.detpoint(dict_system, type=type, threashold=val['threashold'], lt=True)
                            if anomalies:
                                responseD['anomalies'] = anomalies
                                # self.reportAnomaly(responseD)
                            else:
                                logger.info('[%s] : [INFO] No point anomalies detected for type %s ',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), type)
                    if responseD:
                        self.reportAnomaly(responseD)
                    else:
                        logger.info('[%s] : [INFO] No point anomalies detected',
                                    datetime.fromtimestamp(time.time()).strftime(log_format))
                    sleep(parseDelay(self.delay))

    def detectDaskAnomalies(self):
        if str2Bool(self.detect):
            checkpoint = str2Bool(self.checkpoint)
            if self.detecttype == 'clustering':
                logger.info('[{}] : [INFO] Detection with clusterer started. Getting data ...'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                while True:
                    pr_data = self.getDataPR(detect=True)
                    udata = self.filterData(pr_data, detect=True)
                    if self.detectionscaler is not None:
                        logger.info('[{}] : [INFO] Detection scaler set to {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.detectionscaler))
                        scaler_file = os.path.join(self.dataDir, "{}.scaler".format(self.detectionscaler))
                        try:
                            logger.info('[{}] : [INFO] Detection started. Getting data ...'.format(
                                datetime.fromtimestamp(time.time()).strftime(log_format)))
                            sudata = self.dformat.load_scaler(udata, scaler_file)
                        except Exception as inst:
                            logger.warning('[{}] : [WARN] Failed to initialize detection scaler with {} and {}'.format(
                                datetime.fromtimestamp(time.time()).strftime(log_format), type(inst),
                                inst.args))
                            sudata = udata
                    else:
                        sudata = udata
                    if self.augmentations is not None:
                        try:
                            operations = self.augmentations['Operations']
                            try:
                                remove_filtered = self.augmentations['Operations']['RemoveFiltered']
                            except Exception:
                                remove_filtered = True
                        except Exception:
                            operations = False
                            remove_filtered = False
                        asudata = self.dformat.computeOnColumns(sudata, operations=operations, remove_filtered=remove_filtered)
                    else:
                        asudata = sudata
                    if checkpoint:
                        asudata.to_csv(os.path.join(self.dataDir, 'pr_data_detect_augmented.csv'))
                    smodel = sede.SciCluster(modelDir=self.modelsDir, pred_analysis=self.pred_analysis)
                    anomalies = smodel.dask_detect(self.detectmethod, self.load, data=asudata)
                    if not anomalies['anomalies']:
                        logger.info('[{}] : [INFO] No anomalies detected with {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.detectmethod))
                        sleep(parseDelay(self.delay))
                    else:
                        anomalies['method'] = self.detectmethod
                        anomalies['interval'] = self.qinterval
                        logger.info('[{}] : [DEBUG] Reporting detected anomalies: {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), anomalies))
                        self.reportAnomaly(anomalies, dask=True)
                        sleep(parseDelay(self.delay))
            elif self.detecttype == 'classification':
                logger.info('[{}] : [INFO] Detection with classifier started. Getting data ...'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
                while True:
                    pr_data = self.getDataPR(detect=True)
                    udata = self.filterData(pr_data, detect=True)
                    if self.detectionscaler is not None:
                        logger.info('[{}] : [INFO] Detection scaler set to {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.detectionscaler))
                        scaler_file = os.path.join(self.dataDir, "{}.scaler".format(self.detectionscaler))
                        try:
                            logger.info('[{}] : [INFO] Detection started. Getting data ...'.format(
                                datetime.fromtimestamp(time.time()).strftime(log_format)))
                            sudata = self.dformat.load_scaler(udata, scaler_file)
                        except Exception as inst:
                            logger.warning('[{}] : [WARN] Failed to initialize detection scaler with {} and {}'.format(
                                datetime.fromtimestamp(time.time()).strftime(log_format), type(inst),
                                inst.args))
                            sudata = udata
                    else:
                        sudata = udata
                    if self.augmentations is not None:
                        try:
                            operations = self.augmentations['Operations']
                            try:
                                remove_filtered = self.augmentations['Operations']['RemoveFiltered']
                            except Exception:
                                remove_filtered = True
                        except Exception:
                            operations = False
                            remove_filtered = False
                        asudata = self.dformat.computeOnColumns(sudata, operations=operations,
                                                                remove_filtered=remove_filtered)
                    else:
                        asudata = sudata
                    if checkpoint:
                        asudata.to_csv(os.path.join(self.dataDir, 'pr_data_detect_augmented.csv'))
                    classede = cede.SciClassification(self.modelsDir, self.dataDir, self.checkpoint, self.export,
                                                      training=self.trainingSet, validation=self.validationSet,
                                                      validratio=self.validratio, compare=self.compare, cv=self.cv,
                                                      trainscore=self.trainscore, scorers=self.scorers,
                                                      returnestimators=self.returnestimators,
                                                      pred_analysis=self.pred_analysis)
                    anomalies = classede.dask_detect(self.detectmethod, self.load, data=asudata)
                    if not anomalies['anomalies']:
                        logger.info('[{}] : [INFO] No anomalies detected with {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.detectmethod))
                        sleep(parseDelay(self.delay))
                    else:
                        anomalies['method'] = self.detectmethod
                        anomalies['interval'] = self.qinterval
                        logger.info('[{}] : [DEBUG] Reporting detected anomalies: {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), anomalies))
                        self.reportAnomaly(anomalies, dask=True)
                        # print(anomalies)
                        sleep(parseDelay(self.delay))
            else:
                logger.error('[{}] : [ERROR] Unknown detection type {}. Exiting..'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), self.detecttype))
                sys.exit(1)
        else:
            logger.warning('[%s] : [WARN] Detect is set to false, skipping...',
                       datetime.fromtimestamp(time.time()).strftime(log_format))

    def detectAnomalies(self):
        if str2Bool(self.detect):
            checkpoint = str2Bool(self.checkpoint)
            queryd = queryParser(self.query)
            logger.info('[%s] : [INFO] Detection query set as %s ',
                         datetime.fromtimestamp(time.time()).strftime(log_format), str(queryd))
            if self.type == 'clustering':
                while True:
                    logger.info('[{}] : [INFO] Collecting data ...'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format)))
                    systemReturn, yarnReturn, reducemetrics, mapmetrics, mrapp, sparkReturn, stormReturn, cassandraReturn, mongoReturn, userQueryReturn, cepQueryReturn = self.getData(detect=True)

                    if 'yarn' in queryd:
                        yarnReturn = self.filterData(yarnReturn) #todo
                        if checkpoint:
                            dataf = tempfile.NamedTemporaryFile(suffix='.csv')
                            self.dformat.df2csv(yarnReturn, dataf.name)
                            data = dataf.name
                        else:
                            dataf = os.path.join(self.dataDir, 'Final_Merge.csv')
                            data = dataf
                    elif 'storm' in queryd:
                        if checkpoint:
                            data = stormReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'Storm.csv')
                            data = dataf
                        data = self.filterData(data)
                    elif 'userquery' in queryd:
                        if checkpoint:
                            data = userQueryReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'query_response.csv')
                            data = self.dformat.toDF(dataf)
                        data = self.filterData(data)
                    elif 'cep' in queryd:
                        cepQueryReturn = self.filterData(cepQueryReturn)
                        if checkpoint:
                            data = cepQueryReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'CEP.csv')
                            data = self.dformat.toDF(dataf)
                        data = self.filterData(data)
                    elif 'spark' in queryd:
                        sparkReturn = self.filterData(sparkReturn)
                        if checkpoint:
                            data = sparkReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'Spark.csv')
                            data = self.dformat.toDF(dataf)
                        data = self.filterData(data)
                    elif 'system' in queryd:
                        systemReturn = self.filterData(systemReturn)
                        if checkpoint:
                            data = systemReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'System.csv')
                            data = self.dformat.toDF(dataf)
                        data = self.filterData(data)
                    if self.method in self.allowedMethodsClustering:
                        logger.info('[{}] : [INFO] Deteting with selected method {} of type {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format),
                            self.method, self.type))
                        if os.path.isfile(os.path.join(self.modelsDir, self.modelName(self.method, self.load))):
                            logger.info('[{}] : [INFO] model found at {}'.format(
                                datetime.fromtimestamp(time.time()).strftime(log_format),str(
                                os.path.join(self.modelsDir, self.modelName(self.method, self.load)))))
                            wekaList = ['skm', 'em', 'dbscan']
                            if self.method in wekaList:
                                raise Exception("Weka is no longer supported!!")
                            else:
                                smodel = sede.SciCluster(modelDir=self.modelsDir)
                                anomalies = smodel.detect(self.method, self.load, data)
                                if not anomalies['anomalies']:
                                    logger.info('[%s] : [INFO] No anomalies detected with IsolationForest', datetime.fromtimestamp(time.time()).strftime(log_format))
                                    sleep(parseDelay(self.delay))
                                else:
                                    anomalies['method'] = self.method
                                    anomalies['qinterval'] = self.qinterval
                                    self.reportAnomaly(anomalies)
                                    sleep(parseDelay(self.delay))
                        else:
                            logger.error('[%s] : [ERROR] Model %s not found at %s ',
                             datetime.fromtimestamp(time.time()).strftime(log_format), self.load,
                                     str(os.path.join(self.modelsDir, self.modelName(self.method, self.load))))
                            sys.exit(1)
                    else:
                        logger.error('[%s] : [ERROR] Unknown method %s of type %s ',
                                 datetime.fromtimestamp(time.time()).strftime(log_format), self.method,
                                 self.type)
                        sys.exit(1)
            elif self.type == 'classification':
                while True:
                    logger.info('[{}] : [INFO] Collecting data ...'.format(
                        datetime.fromtimestamp(time.time()).strftime(log_format)))
                    systemReturn, yarnReturn, reducemetrics, mapmetrics, mrapp, sparkReturn, stormReturn, cassandraReturn, mongoReturn, userQueryReturn, cepQueryReturn = self.getData(
                        detect=True)
                    if 'yarn' in queryd:
                        # yarnReturn = self.filterData(yarnReturn)  # todo
                        if checkpoint:
                            data = yarnReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'Final_Merge.csv')
                            data = self.dformat.toDF(dataf)
                            data.set_index('key', inplace=True)
                        data = self.filterData(data)
                    elif 'storm' in queryd:
                        if checkpoint:
                            data = stormReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'Storm.csv')
                            data = self.dformat.toDF(dataf)
                            data.set_index('key', inplace=True)
                        data = self.filterData(data)
                    elif 'userquery' in queryd:
                        if checkpoint:
                            data = userQueryReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'query_response.csv')
                            data = self.dformat.toDF(dataf)
                            data.set_index('key', inplace=True)
                        data = self.filterData(data)
                    elif 'cep' in queryd:
                        # cepQueryReturn = self.filterData(cepQueryReturn)
                        if checkpoint:
                            data = cepQueryReturn
                        else:
                            dataf = os.path.join(self.dataDir, 'CEP.csv')
                            data = self.dformat.toDF(dataf)
                        data.set_index('key', inplace=True)
                        data = self.filterData(data)
                    elif 'spark' in queryd:
                        if checkpoint:
                            data = sparkReturn
                        else:
                            dataf = os.path.joint(self.dataDir, 'Spark.csv')
                            data = self.dformat.toDF(dataf)
                        data.set_index('key', inplace=True)
                        data = self.filterData(data)
                    if self.method in self.allowefMethodsClassification:
                        logger.info('[{}] : [INFO] Deteting with selected method {} of type {}'.format(
                            datetime.fromtimestamp(time.time()).strftime(log_format), self.method, self.type))
                        if os.path.isfile(os.path.join(self.modelsDir, self.modelName(self.method, self.load))):
                            logger.info('[{}] : [INFO] Model found at {}'.format(
                                datetime.fromtimestamp(time.time()).strftime(log_format), str(
                                os.path.join(self.modelsDir, self.modelName(self.method, self.load)))))
                            cmodel = cede.SciClassification(self.modelsDir, self.dataDir, self.checkpoint, self.export,
                                                            training=self.trainingSet, validation=self.validationSet,
                                                            validratio=self.validratio, compare=self.compare)
                            anomalies = cmodel.detect(self.method, self.load, data)
                            if not anomalies['anomalies']:
                                logger.info('[%s] : [INFO] No anomalies detected with %s',
                                            datetime.fromtimestamp(time.time()).strftime(log_format), str(self.method))
                                sleep(parseDelay(self.delay))
                            else:
                                anomalies['method'] = self.method
                                anomalies['qinterval'] = self.qinterval
                                self.reportAnomaly(anomalies)
                                sleep(parseDelay(self.delay))
                        else:
                            logger.error('[%s] : [ERROR] Model %s not found at %s ',
                                         datetime.fromtimestamp(time.time()).strftime(log_format), self.load,
                                         str(os.path.join(self.modelsDir, self.modelName(self.method, self.load))))
                            sys.exit(1)

                    else:
                        logger.error('[%s] : [ERROR] Unknown method %s of type %s ',
                                     datetime.fromtimestamp(time.time()).strftime(log_format), self.method,
                                     self.type)
                        sys.exit(1)

                # sys.exit(0)
            else:
                logger.error('[%s] : [ERROR] Unknown type %s ',
                         datetime.fromtimestamp(time.time()).strftime(log_format), self.type)
                sys.exit(1)
        else:
            logger.warning('[%s] : [WARN] Detect is set to false, skipping...',
                       datetime.fromtimestamp(time.time()).strftime(log_format))

    def run(self, engine):
        try:
            threadPoint = EdePointThread(engine, 'Point')
            threadTrain = EdeTrainThread(engine, 'Train')
            threadDetect = EdeDetectThread(engine, 'Detect')

            threadPoint.start()
            threadTrain.start()
            threadDetect.start()

            threadPoint.join()
            threadTrain.join()
            threadDetect.join()
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception %s with %s during thread execution, halting',
                           datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(1)
        return 0

    def runProcess(self, engine):
        proc = []
        try:
            if str2Bool(self.point):
                pPoint = EdePointProcess(engine, 'Point Proc')

            pTrain = EdeTrainProcess(engine, 'Train Proc')
            pDetect = EdeDetectProcess(engine, 'Detect Proc')

            if str2Bool(self.point):
                processPoint = pPoint.run()
                proc.append(processPoint)

            processTrain = pTrain.run()
            proc.append(processTrain)
            processDetect = pDetect.run()
            proc.append(processDetect)

            if str2Bool(self.point):
                processPoint.start()
            processTrain.start()
            processDetect.start()

            for p in proc:
                p.join()
                print('%s.exitcode = %s' % (p.name, p.exitcode))

        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception %s with %s during process execution, halting',
                           datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(1)
        return 0

    def runDask(self, engine):
        try:
            if str2Bool(self.point):
                print("point dask")
            dtrain = engine.trainDaskMethod()
            ddetect = engine.detectDaskAnomalies()
            time.sleep(10)
        except Exception as inst:
            logger.error('[{}] : [ERROR] Exception while running Dask backend with {} and {}, halting'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(1)

    def modelName(self, methodname, modelName):
        '''
        :param methodname: -> name of current method (self.method)
        :param modelName: ->name of current export (self.export)
        :return:
        '''
        saveName = "%s_%s.model" %(methodname, modelName)
        if not os.path.isfile(os.path.join(self.modelsDir, saveName)):
            saveName = "%s_%s.pkl" %(methodname, modelName)
        return saveName

    def pushModel(self):
        return "model"

    def compareModel(self):
        return "Compare models"

    def reportAnomaly(self, body, dask=False):
        now = datetime.utcnow()
        itime = now.strftime("%Y-%m-%dT%H:%M:%S") + ".%03d" % (now.microsecond / 1000) + "Z"
        body["reporttimestamp"] = itime
        if not dask:
            self.edeConnector.pushAnomalyES(anomalyIndex=self.anomalyIndex, doc_type='anomaly', body=body)
        else:
            self.edeConnector.pushAnomalyKafka(body=body)
        if self.grafana_url:
            from edereporting.edegrafana import EDEGrafanaDash
            logger.info('[%s] : [INFO] Adding Grafana annotation ...',
                                            datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            det_method = body['method']
            det_model = body['model']
            anomaly_method = f"Anomalies detected by EDE with method {det_method}, model: {det_model}"
            anomaly_tags = [f'{det_method}_anomaly']
            ede_grafana = EDEGrafanaDash(grafana_token=self.grafana_credentials, grafana_url=self.grafana_url)
            # Check if dashboard exists with tag
            dash_uid, dash_url, dash_id = ede_grafana.get_dash(tag=self.grafana_tag)
            if dash_uid:
                logger.info('[{}] : [INFO] Detected Grafana dashboard with tag {}, marking anomalies ... '.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.grafana_tag))
            else:
                # Generate Demo Dashboard
                logger.info('[{}] : [INFO] Grafana Dashoard with tag {} not detected, creating ... '.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.grafana_tag))
                ede_grafana.generate_dash(tag=self.grafana_tag, title="Serrano Grafana Dash Demo v1")
                ede_grafana.create_dash()
                # Getting dash data
                dash_uid, dash_url, dash_id = ede_grafana.get_dash(tag=self.grafana_tag)
                logger.info(
                    '[{}] : [INFO] Grafana Dashoard generated with tag {}, uid {}, marking anomalies ... '.format(
                        datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.grafana_tag, dash_uid))
            for anomaly in body['anomalies']: # todo if anomalies are sequential make one continouse annotation not separate ones
                try:
                    anomaly_type = anomaly['type']
                    anomaly_tags = [f'{anomaly_type}_{det_method}_anomaly']
                except:
                    pass
                ede_grafana.push_annotation(int(anomaly['utc'] * 1000), int(anomaly['utc'] * 1000),
                                            anomaly_tags=anomaly_tags,
                                            message=anomaly_method, dash_id=dash_id)

    def getDFS(self, detect=False):
        # Query Strings
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying DFS metrics")
        logger.info('[%s] : [INFO] Querying DFS metrics...',
                                            datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        dfs, dfs_file = self.qConstructor.dfsString()
        dfsFs, dfsFs_file = self.qConstructor.dfsFString()
        fsop, fsop_file = self.qConstructor.fsopDurationsString()

        # Query constructor
        qdfs = self.qConstructor.dfsQuery(dfs, tfrom, to, self.qsize, self.qinterval)
        qdfsFs = self.qConstructor.dfsFSQuery(dfsFs, tfrom, to, self.qsize, self.qinterval)
        qfsop = self.qConstructor.fsopDurationsQuery(fsop, tfrom, to, self.qsize, self.qinterval)

        # Execute query
        gdfs = self.edeConnector.aggQuery(qdfs)
        gdfsFs = self.edeConnector.aggQuery(qdfsFs)
        gfsop = self.edeConnector.aggQuery(qfsop)

        if not checkpoint:
            self.dformat.dict2csv(gdfs, qdfs, dfs_file)
            self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file)
            self.dformat.dict2csv(gfsop, qfsop, fsop_file)
        else:
            df_dfs = self.dformat.dict2csv(gdfs, qdfs, dfs_file, df=checkpoint)
            df_dfsFs = self.dformat.dict2csv(gdfsFs, qdfsFs, dfsFs_file, df=checkpoint)
            df_fsop = self.dformat.dict2csv(gfsop, qfsop, fsop_file, df=checkpoint)

        print("Querying DFS metrics complete.")
        logger.info('[%s] : [INFO] Querying DFS metrics complete.',
                    datetime.fromtimestamp(time.time()).strftime(log_format))

        print("Starting DFS merge ...")
        if not checkpoint:
            merged_DFS = self.dformat.chainMergeDFS()
            self.dformat.df2csv(merged_DFS, os.path.join(self.dataDir, 'Merged_DFS.csv'))
            logger.info('[%s] : [INFO] DFS merge complete',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
            print("DFS merge complete.")
            return 0
        else:
            merged_DFS = self.dformat.chainMergeDFS(dfs=df_dfs, dfsfs=df_dfsFs, fsop=df_fsop)
            logger.info('[%s] : [INFO] DFS merge complete',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
            print("DFS merge complete.")
            return merged_DFS

    def getNodeManager(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying  Node Manager and Shuffle metrics ...")
        logger.info('[%s] : [INFO] Querying  Node Manager and Shuffle metrics...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        lNM = []
        ljvmNM = []
        lShuffle = []
        for node in nodes:
            nodeManager, nodeManager_file = self.qConstructor.nodeManagerString(node)
            jvmNodeManager, jvmNodeManager_file = self.qConstructor.jvmnodeManagerString(node)
            shuffle, shuffle_file = self.qConstructor.shuffleString(node)

            qnodeManager = self.qConstructor.yarnNodeManager(nodeManager, tfrom, to, self.qsize,
                                                             self.qinterval)
            qjvmNodeManager = self.qConstructor.jvmNNquery(jvmNodeManager, tfrom, to, self.qsize,
                                                           self.qinterval)
            qshuffle = self.qConstructor.shuffleQuery(shuffle, tfrom, to, self.qsize, self.qinterval)

            gnodeManagerResponse = self.edeConnector.aggQuery(qnodeManager)
            if list(gnodeManagerResponse['aggregations'].values())[0].values()[0]:
                if not checkpoint:
                    self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file)
                else:
                    lNM.append(self.dformat.dict2csv(gnodeManagerResponse, qnodeManager, nodeManager_file, df=checkpoint))
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                            datetime.fromtimestamp(time.time()).strftime(log_format), node)

            gjvmNodeManagerResponse = self.edeConnector.aggQuery(qjvmNodeManager)
            if list(gjvmNodeManagerResponse['aggregations'].values())[0].values()[0]:
                if not checkpoint:
                    self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file)
                else:
                    ljvmNM.append(self.dformat.dict2csv(gjvmNodeManagerResponse, qjvmNodeManager, jvmNodeManager_file, df=checkpoint))
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no Node Manager detected!',
                            datetime.fromtimestamp(time.time()).strftime(log_format), node)

            gshuffleResponse = self.edeConnector.aggQuery(qshuffle)
            if list(gshuffleResponse['aggregations'].values())[0].values()[0]:
                if not checkpoint:
                    self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file)
                else:
                    lShuffle.append(self.dformat.dict2csv(gshuffleResponse, qshuffle, shuffle_file, df=checkpoint))
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no shuffle metrics!',
                            datetime.fromtimestamp(time.time()).strftime(log_format), node)
        print("Querying  Node Manager and Shuffle metrics complete.")
        logger.info('[%s] : [INFO] Querying  Node Manager and Shuffle metrics complete...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))

        print("Starting Node Manager merge ...")
        if not checkpoint:
            nm_merged, jvmnn_merged, shuffle_merged = self.dformat.chainMergeNM()
            self.dformat.df2csv(nm_merged, os.path.join(self.dataDir, 'Merged_NM.csv'))
            self.dformat.df2csv(jvmnn_merged, os.path.join(self.dataDir, 'Merged_JVM_NM.csv'))
            self.dformat.df2csv(shuffle_merged, os.path.join(self.dataDir, 'Merged_Shuffle.csv'))
            logger.info('[%s] : [INFO] Node Manager Merge complete',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
            print("Node Manager Merge Complete")
            nm_merged = 0
            jvmnn_merged = 0
            shuffle_merged = 0
        else:
            nm_merged, jvmnn_merged, shuffle_merged = self.dformat.chainMergeNM(lNM=lNM, lNMJvm=ljvmNM, lShuffle=lShuffle)
            logger.info('[%s] : [INFO] Node Manager Merge complete',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
            print("Node Manager Merge Complete")
        return nm_merged, jvmnn_merged, shuffle_merged

    def getNameNode(self, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying  Name Node metrics ...")
        logger.info('[%s] : [INFO] Querying  Name Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        jvmNameNodeString, jvmNameNode_file = self.qConstructor.jvmNameNodeString()
        qjvmNameNode = self.qConstructor.jvmNNquery(jvmNameNodeString, tfrom, to, self.qsize, self.qinterval)
        gjvmNameNode = self.edeConnector.aggQuery(qjvmNameNode)
        if not checkpoint:
            self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file)
            returnNN = 0
        else:
            df_NN = self.dformat.dict2csv(gjvmNameNode, qjvmNameNode, jvmNameNode_file, df=checkpoint)
            # df_NN.set_index('key', inplace=True)
            returnNN = df_NN
        print("Querying  Name Node metrics complete")
        logger.info('[%s] : [INFO] Querying  Name Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        return returnNN

    def getCluster(self, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying  Cluster metrics ...")
        logger.info('[%s] : [INFO] Querying  Name Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        queue, queue_file = self.qConstructor.queueResourceString()
        cluster, cluster_file = self.qConstructor.clusterMetricsSring()
        
        jvmResMng, jvmResMng_file = self.qConstructor.jvmResourceManagerString()

        
        qqueue = self.qConstructor.resourceQueueQuery(queue, tfrom, to, self.qsize, self.qinterval)
        qcluster = self.qConstructor.clusterMetricsQuery(cluster, tfrom, to, self.qsize, self.qinterval)
        qjvmResMng = self.qConstructor.jvmNNquery(jvmResMng, tfrom, to, self.qsize, self.qinterval)

        gqueue = self.edeConnector.aggQuery(qqueue)
        gcluster = self.edeConnector.aggQuery(qcluster)
        
        gjvmResourceManager = self.edeConnector.aggQuery(qjvmResMng)

        if not checkpoint:
            self.dformat.dict2csv(gcluster, qcluster, cluster_file)
            self.dformat.dict2csv(gqueue, qqueue, queue_file)
            
            self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file)

            print("Starting cluster merge ...")
            merged_cluster = self.dformat.chainMergeCluster()
            self.dformat.df2csv(merged_cluster, os.path.join(self.dataDir, 'Merged_Cluster.csv'))
            clusterReturn = 0
        else:
            df_cluster = self.dformat.dict2csv(gcluster, qcluster, cluster_file, df=checkpoint)
            df_queue = self.dformat.dict2csv(gqueue, qqueue, queue_file, df=checkpoint)
            
            df_jvmResourceManager = self.dformat.dict2csv(gjvmResourceManager, qjvmResMng, jvmResMng_file, df=checkpoint)
            print("Starting cluster merge ...")
            merged_cluster = self.dformat.chainMergeCluster(clusterMetrics=df_cluster, queue=df_queue,
                                                                   jvmRM=df_jvmResourceManager)
            clusterReturn = merged_cluster
        print("Querying  Cluster metrics complete")
        logger.info('[%s] : [INFO] Querying  Name Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))

        logger.info('[%s] : [INFO] Cluster Merge complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        print("Cluster merge complete")
        return clusterReturn

    def getMapnReduce(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        # per slave unique process name list
        nodeProcessReduce = {}
        nodeProcessMap = {}
        lRD = {}
        lMP = {}
        checkpoint = str2Bool(self.checkpoint)
        print("Querying  Mapper and Reducer metrics ...")
        logger.info('[%s] : [INFO] Querying  Mapper and Reducer metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        for node in nodes:
            reduce = self.qConstructor.jvmRedProcessString(node)
            map = self.qConstructor.jvmMapProcessingString(node)

            qreduce = self.qConstructor.queryByProcess(reduce, tfrom, to, 500, self.qinterval)
            qmap = self.qConstructor.queryByProcess(map, tfrom, to, 500, self.qinterval)

            greduce = self.edeConnector.aggQuery(qreduce)
            gmap = self.edeConnector.aggQuery(qmap)

            uniqueReduce = set()
            for i in greduce['hits']['hits']:
                uniqueReduce.add(i['_source']['ProcessName'])
            nodeProcessReduce[node] = list(uniqueReduce)

            uniqueMap = set()
            for i in gmap['hits']['hits']:
                uniqueMap.add(i['_source']['ProcessName'])
            nodeProcessMap[node] = list(uniqueMap)

        print("Querying  Reducer metrics ...")
        logger.info('[%s] : [INFO] Querying  Reducer metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        for host, processes in nodeProcessReduce.items():
            if processes:
                for process in processes:
                    logger.info('[%s] : [INFO] Reduce process %s for host  %s found',
                                datetime.fromtimestamp(time.time()).strftime(log_format), process,
                                host)
                    hreduce, hreduce_file = self.qConstructor.jvmRedProcessbyNameString(host, process)
                    qhreduce = self.qConstructor.jvmNNquery(hreduce, tfrom, to, self.qsize, self.qinterval)
                    ghreduce = self.edeConnector.aggQuery(qhreduce)
                    if not checkpoint:
                        self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file)
                    else:
                        lRD[process] = self.dformat.dict2csv(ghreduce, qhreduce, hreduce_file, df=checkpoint)
            else:
                logger.info('[%s] : [INFO] No reduce process for host  %s found',
                            datetime.fromtimestamp(time.time()).strftime(log_format), host)
                pass
        print("Querying  Reducer metrics complete")
        logger.info('[%s] : [INFO] Querying  Reducer metrics complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))

        print("Querying  Mapper metrics ...")
        logger.info('[%s] : [INFO] Querying  Mapper metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        for host, processes in nodeProcessMap.items():
            if processes:
                for process in processes:
                    logger.info('[%s] : [INFO] Map process %s for host  %s found',
                                datetime.fromtimestamp(time.time()).strftime(log_format), process,
                                host)
                    hmap, hmap_file = self.qConstructor.jvmMapProcessbyNameString(host, process)
                    qhmap = self.qConstructor.jvmNNquery(hmap, tfrom, to, self.qsize, self.qinterval)
                    ghmap = self.edeConnector.aggQuery(qhmap)
                    if not checkpoint:
                        self.dformat.dict2csv(ghmap, qhmap, hmap_file)
                    else:
                        lMP[process] = self.dformat.dict2csv(ghmap, qhmap, hmap_file, df=checkpoint)
            else:
                logger.info('[%s] : [INFO] No map process for host  %s found',
                            datetime.fromtimestamp(time.time()).strftime(log_format), host)
                pass
        print("Querying  Mapper metrics complete")
        logger.info('[%s] : [INFO] Querying  Mapper metrics complete',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
        print("Querying MRApp metrics ... ")
        logger.info('[%s] : [INFO] Querying  MRApp metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        jvmMrapp, jvmMrapp_file = self.qConstructor.jvmMrappmasterString()

        qjvmMrapp = self.qConstructor.jvmNNquery(jvmMrapp, tfrom, to, self.qsize, self.qinterval)
        gjvmMrapp = self.edeConnector.aggQuery(qjvmMrapp)

        if not checkpoint:
            self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file)
            df_jvmMrapp = 0
        else:
            df_jvmMrapp = self.dformat.dict2csv(gjvmMrapp, qjvmMrapp, jvmMrapp_file, df=checkpoint)
        logger.info('[%s] : [INFO] Querying  MRApp metrics complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        print("Querying MRApp metrics complete ")
        return lMP, lRD, df_jvmMrapp

    def getDataNode(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying  Data Node metrics ...")
        logger.info('[%s] : [INFO] Querying  Data Node metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        lDN = []
        for node in nodes:
            datanode, datanode_file = self.qConstructor.datanodeString(node)
            qdatanode = self.qConstructor.datanodeMetricsQuery(datanode, tfrom, to, self.qsize,
                                                               self.qinterval)
            gdatanode = self.edeConnector.aggQuery(qdatanode)
            if list(gdatanode['aggregations'].values())[0].values()[0]:
                if not checkpoint:
                    self.dformat.dict2csv(gdatanode, qdatanode, datanode_file)
                else:
                    lDN.append(self.dformat.dict2csv(gdatanode, qdatanode, datanode_file, df=checkpoint))
            else:
                logger.info('[%s] : [INFO] Empty response from  %s no datanode metrics!',
                            datetime.fromtimestamp(time.time()).strftime(log_format), node)
        print("Querying  Data Node metrics complete")
        if detect:
            self.dataNodeDetecting = len(lDN)
        else:
            self.dataNodeTraining = len(lDN)
        logger.info('[%s] : [INFO] Querying  Data Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))

        print("Starting Data Node metrics merge ...")
        if not checkpoint:
            dn_merged = self.dformat.chainMergeDN()
            self.dformat.df2csv(dn_merged, os.path.join(self.dataDir, 'Merged_DN.csv'))
            logger.info('[%s] : [INFO] Data Node metrics merge complete',
                            datetime.fromtimestamp(time.time()).strftime(log_format))
            print("Data Node metrics merge complete")
            return 0
        else:
            dn_merged = self.dformat.chainMergeDN(lDN=lDN)
            logger.info('[%s] : [INFO] Data Node metrics merge complete',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
            print("Data Node metrics merge complete")
            return dn_merged

    def getCassandra(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying  Cassandra metrics ...")
        logger.info('[%s] : [INFO] Querying  Cassandra metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        lcassandraCounter = []
        lcassandraGauge = []
        for node in nodes:
            cassandra, cassandra_file = self.qConstructor.cassandraCounterString(host=node)
            cassandragauge, cassandragauge_file = self.qConstructor.cassandraGaugeString(host=node)

            # Queries
            qcassandra = self.qConstructor.cassandraQuery(cassandra, tfrom, to, self.qsize, self.qinterval)
            qcassandragauge = self.qConstructor.cassandraQuery(cassandragauge, tfrom, to, self.qsize, self.qinterval)

            # Execute query and convert response to csv
            gcassandra = self.edeConnector.aggQuery(qcassandra)
            gcassandragauge = self.edeConnector.aggQuery(qcassandragauge)


            lcassandraCounter.append(self.dformat.dict2csv(gcassandra, qcassandra, cassandra_file, df=True))
            lcassandraGauge.append(
                self.dformat.dict2csv(gcassandragauge, qcassandragauge, cassandragauge_file, df=True))

            # Merge and rename by node system Files
        df_CA_Count = self.dformat.chainMergeCassandra(lcassandraCounter)
        df_CA_Gauge = self.dformat.chainMergeCassandra(lcassandraGauge)

        df_CA = self.dformat.listMerge([df_CA_Count, df_CA_Gauge])
        print("Cassandra  metrics merge complete")
        logger.info('[%s] : [INFO] Cassandra  metrics merge complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        if not checkpoint:
            self.dformat.df2csv(df_CA, os.path.join(self.dataDir, 'Merged_Cassandra.csv'))
            return 0
        else:
            return df_CA

    def getMongodb(self, nodes, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying Mongodb metrics ...")
        logger.info('[%s] : [INFO] Querying  MongoDB metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        lmongoCounter = []
        lmongoGauge = []
        for node in nodes:
            mongodbCounter, mongodbCounter_file = self.qConstructor.mongodbCounterString(host=node)
            mongodbGauge, mongodbGauge_file = self.qConstructor.mongodbGaugeString(host=node)

            #Queries
            qmongodbCounter = self.qConstructor.mongoDBCounterQuery(mongodbCounter, tfrom, to, self.qsize, self.qinterval)
            qmongodbGauge = self.qConstructor.mongoDBGaugeQuery(mongodbGauge, tfrom, to, self.qsize, self.qinterval)

            # Execute query and convert response to csv
            gmongodbGauge = self.edeConnector.aggQuery(qmongodbGauge)
            gmongodbCounter = self.edeConnector.aggQuery(qmongodbCounter)

            lmongoCounter.append(self.dformat.dict2csv(gmongodbCounter, qmongodbCounter, mongodbCounter_file))
            lmongoGauge.append(self.dformat.dict2csv(gmongodbGauge, qmongodbGauge, mongodbGauge_file))


        #Merge and rename by node system File
        df_MD_Count = self.dformat.chainMergeMongoDB(lmongoCounter)
        df_MD_Gauge = self.dformat.chainMergeMongoDB(lmongoGauge)

        df_MD = self.dformat.listMerge([df_MD_Count, df_MD_Gauge])
        print("MongoDB metrics merged")
        logger.info('[%s] : [INFO] MongoDB  metrics merge complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))

        if not checkpoint:
            self.dformat.df2csv(df_MD, os.path.join(self.dataDir, "Merged_Mongo.csv"))
            return 0
        else:
            return df_MD

    def getQuery(self, detect=False):
        if not os.path.isfile(os.path.join(self.queryDir, 'query.json')):
            logger.error('[%s] : [ERROR] No user defined query found in queries directory!',
                        datetime.fromtimestamp(time.time()).strftime(log_format))
            print("No user defined query found in queries directory! Exiting ...")
            sys.exit(1)
        logger.info('[%s] : [INFO] Started User defined querying  ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        query = self.qConstructor.sideQuery()
        try:
            # Query string
            queryStr =  query['query']['filtered']['query']['query_string']['query']

            # Query range
            if detect:
                query['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] = "now-%s" % self.interval
                query['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = "now"
                logger.info('[%s] : [INFO] User defined query detect set with interval %s!',
                            datetime.fromtimestamp(time.time()).strftime(log_format),
                            str(self.interval))
            qfrom = query['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte']
            qto = query['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte']

            # Query Size
            qSize = query['size']

            # Query Aggs
            if len(list(query['aggs'].values())) > 1:
                logger.error('[%s] : [ERROR] Aggregation type unsupported, got length %s expected 1!',
                            datetime.fromtimestamp(time.time()).strftime(log_format), str(len(list(query['aggs'].values()))))
                print("Aggregation type unsupported, got length %s expected 1!" % str(len(list(query['aggs'].values()))))
                sys.exit(1)
            else:
                qInterval = list(query['aggs'].values())[0]['date_histogram']['interval']
                if detect:
                    list(query['aggs'].values())[0]['date_histogram']['extended_bounds']['min'] = "now-%s" % self.interval
                    list(query['aggs'].values())[0]['date_histogram']['extended_bounds']['max'] = "now"
                qMin = list(query['aggs'].values())[0]['date_histogram']['extended_bounds']['min']
                qMax = list(query['aggs'].values())[0]['date_histogram']['extended_bounds']['max']
        except Exception as inst:
            logger.error('[%s] : [ERROR] Unsupported query detected with %s and %s!',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            print("Unsupported query detected! Exiting ...")
            sys.exit(1)
        logger.info('[%s] : [INFO] Query succesfully parsed; querystring -> %s, from-> %s, to-> %s, size-> %s, interval-> %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), queryStr, qfrom, qto, qSize, qInterval)
        print("User Query Succesfully parsed: ")
        print("querystring -> %s" % queryStr)
        print("from-> %s" % qfrom)
        print("to-> %s" % qto)
        print("size-> %s" % qSize)
        print("interval-> %s" % qInterval)
        response_file = self.qConstructor.sideQueryString()
        guserQuery = self.edeConnector.aggQuery(query)

        if not checkpoint:
            self.dformat.dict2csv(guserQuery, query, response_file)
            returnUQ = 0
        else:
            df_UQ = self.dformat.dict2csv(guserQuery, query, response_file, df=checkpoint)
            returnUQ = df_UQ
        print("Querying  Name Node metrics complete")
        logger.info('[%s] : [INFO] Querying  Name Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        return returnUQ

    def getCEP(self, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying CEP metrics ...")
        logger.info('[%s] : [INFO] Querying  CEP metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)
        cep, cep_file = self.qConstructor.cepQueryString()

        # Queries
        qcep = self.qConstructor.cepQuery(cep, tfrom, to, self.qsize, self.interval, qmin_doc_count=0)

        # Execute query and convert response to csv
        respMetrics, gcep = self.edeConnector.query(queryBody=qcep)
        print(gcep)
        dCepArray = []
        try:
            for el in gcep['hits']['hits']:
                try:
                    dCep = {}
                    dCep['ms'] = el['_source']['ms']
                    dCep['key'] = el['_source']['@timestamp']
                    dCep['component'] = el['_source']['Component']
                    dCep['host'] = el['_source']['host']
                    dCep['ship'] = el['_source']['ship']
                    dCep['method'] = el['_source']['method']
                    dCepArray.append(dCep)
                except Exception as inst:
                    print('Failed to parse CEP response!')
                    logger.warning('[%s] : [WARN] Failed to parse CEP response with %s and %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
        except Exception as inst:
            print('Malformed CEP response detected. Exiting!')
            logger.error('[%s] : [ERROR] Malformed CEP response detected  with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(1)
        if not dCepArray:
            print("CEP response is empty! Exiting ....")
            logger.error('[%s] : [WARN] CEP response is empty!',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
            sys.exit(1)
        df = self.dformat.dtoDF(dCepArray)
        if not checkpoint:
            self.dformat.df2csv(df, os.path.join(self.dataDir, cep_file))
            returnCEP = 0
        else:
            returnCEP = df
        print("Querying  CEP metrics complete")
        logger.info('[%s] : [INFO] Querying  CEP metrics complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))

        return returnCEP

    def getSpark(self, detect=False):
        if detect:
            tfrom = "now-%s" % self.interval
            to = "now"
        else:
            tfrom = int(self.tfrom)
            to = int(self.to)
        print("Querying Spark metrics ...")
        logger.info('[%s] : [INFO] Querying  Spark metrics ...',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        checkpoint = str2Bool(self.checkpoint)

        sparkString, spark_file = self.qConstructor.sparkString()
        qSpark = self.qConstructor.sparkQuery(sparkString, tfrom, to, self.qsize, self.interval)
        gSpark = self.edeConnector.aggQuery(qSpark)

        if not checkpoint:
            self.dformat.dict2csv(gSpark, qSpark, spark_file)
            returnSP = 0
        else:
            df_SP = self.dformat.dict2csv(gSpark, qSpark, spark_file, df=checkpoint)
            returnNN = df_SP
        print("Querying  Spark metrics complete")
        logger.info('[%s] : [INFO] Querying  Name Node metrics complete',
                    datetime.fromtimestamp(time.time()).strftime(log_format))
        return returnNN


    def print_time(self, threadName, delay):
        count = 0
        while count < 5:
            time.sleep(delay)
            count += 1
            print("%s: %s" % (threadName, time.ctime(time.time())))




