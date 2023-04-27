"""
Copyright 2021, Institute e-Austria, Timisoara, Romania
    https://www.ieat.ro/
Developers:
 * Gabriel Iuhasz, iuhasz.gabriel@info.uvt.ro

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:
    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from datetime import datetime
from elasticsearch import Elasticsearch
from kafka import KafkaProducer
import pandas as pd
import requests
import os
import sys
from edelogger import logger
import json
import time
from requests.auth import HTTPBasicAuth
from util import log_format
from joblib import Parallel, delayed
from tqdm import tqdm

class Connector:
    def __init__(self,
                 prEndpoint=None,
                 prEndpointUser=None,
                 prEndpointPasswd=None,
                 esEndpoint=None,
                 dmonPort=5001,
                 MInstancePort=9200,
                 index="logstash-*",
                 prKafkaEndpoint=None,
                 prKafkaPort=9092,
                 prKafkaTopic='edetopic',
                 srTelemetryPMDS=None,
                 central_telemetry_handler='http://central-telemetry.services.cloud.ict-serrano.eu/',
                 enhanced_telemetry_agent='http://85.120.206.26:30090'
                 ):
        self.dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        if esEndpoint is None:
            self.esInstance = None
        else:
            self.esInstance = Elasticsearch(esEndpoint)
            self.esEndpoint = esEndpoint
            self.dmonPort = dmonPort
            self.esInstanceEndpoint = MInstancePort
            self.myIndex = index
            logger.info('[{}] : [INFO] EDE ES backend Defined at: {} with port {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), esEndpoint, MInstancePort))
        if prEndpoint is None:
            pass
        else:
            self.prEndpoint = prEndpoint
            self.MInstancePort = MInstancePort
            logger.info('[{}] : [INFO] EDE PR backend Defined at: {} with port {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), prEndpoint, MInstancePort))
            self.prEndpointUser = prEndpointUser
            self.prEndpointPasswd = prEndpointPasswd
            if self.prEndpointUser is not None:
                logger.info('[{}] : [INFO] EDE PR user defined'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
            if self.prEndpointPasswd is not None:
                logger.info('[{}] : [INFO] EDE PR passwd defined'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
            self.dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        if prKafkaEndpoint is None:
            self.producer = None
            logger.warning('[{}] : [WARN] EDE Kafka reporter not set'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
        else:
            self.prKafkaTopic = prKafkaTopic
            try:
                self.producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                              bootstrap_servers=["{}:{}".format(prKafkaEndpoint, prKafkaPort)],
                                              retries=5)
                logger.info('[{}] : [INFO] EDE Kafka reporter initialized to server {}:{}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), prKafkaEndpoint, prKafkaPort))
            except Exception as inst:
                logger.error('[{}] : [ERROR] EDE Kafka reporter failed with {} and {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
                self.producer = None
        if srTelemetryPMDS is None:
            self.srTelemetryPMDS = os.getenv('PMDS_SERVICE', 'http://pmds.services.cloud.ict-serrano.eu')
        else:
            self.srTelemetryPMDS = srTelemetryPMDS
        self.central_telemetry_handler = central_telemetry_handler
        self.enhanced_telemetry_agent = enhanced_telemetry_agent

    def pr_health_check(self):
        pr_target_health = '/-/healthy'
        pr_target_ready = '/-/ready'
        try:
            if self.__check_auth_pr():
                resp_h = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_health),
                                      auth=HTTPBasicAuth(self.prEndpointUser, self.prEndpointPasswd))
                resp_r = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_ready),
                                      auth=HTTPBasicAuth(self.prEndpointUser, self.prEndpointPasswd))
            else:
                resp_h = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_health))
                resp_r = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_ready))
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has occured while connecting to PR endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(2)
        if resp_h.status_code != 200:
            logger.error(
                '[{}] : [ERROR] PR endpoint health is bad, exiting'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
            sys.exit(2)
        if resp_r.status_code != 200:
            logger.error(
                '[{}] : [ERROR] PR endpoint not ready to serve traffic'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format)))
            sys.exit(2)
        logger.info(
            '[{}] : [INFO] PR endpoint healthcheck pass'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format)))
        return resp_h.status_code, resp_r.status_code

    def pr_status(self, type=None):
        """
        Get status of prometheus

        TODO: check runtimeinfo and flags
        :param type: suported types
        :return:
        """
        suported = ['runtimeinfo', 'config', 'flags']
        if type is None:
            pr_target_string = '/api/v1/status/config'
        elif type in suported:
            pr_target_string = '/api/v1/status/{}'.format(type)
        else:
            logger.error('[{}] : [ERROR] unsupported status type {}, supported types are {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), type, suported))
            sys.exit(1)
        try:
            if self.__check_auth_pr():
                resp = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_string),
                                    auth=HTTPBasicAuth(self.prEndpointUser, self.prEndpointPasswd))
            else:
                resp = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_string))
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has occured while connecting to PR endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(2)
        return resp.json()

    def pr_targets(self):
        """
        Get Monitored Target Info
        :return: Targets Dict
        """
        pr_target_string = '/api/v1/targets'
        try:
            if self.__check_auth_pr():
                resp = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_string),
                                    auth=HTTPBasicAuth(self.prEndpointUser, self.prEndpointPasswd))
            else:
                resp = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_string))
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has occured while connecting to PR endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(2)
        return resp.json()

    def pr_labels(self, label=None):
        if label is None:
            pr_target_string = '/api/v1/labels'
        else:
            pr_target_string = '/api/v1/label/{}/values'.format(label)
        try:
            if self.__check_auth_pr():
                resp = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_string),
                                    auth=HTTPBasicAuth(self.prEndpointUser, self.prEndpointPasswd))
            else:
                resp = requests.get("https://{}:{}{}".format(self.prEndpoint, self.MInstancePort, pr_target_string))
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has occured while connecting to PR endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(2)
        return resp.json()

    def pr_query(self, query):
        """
        QUery Monitoring Data From PR backend
        :param query: Query string for PR backend
        :return: Monitoring Data
        """
        try:
            url = '/api/v1/query'
            if self.__check_auth_pr():
                resp = requests.get('https://{}:{}{}'.format(self.prEndpoint, self.MInstancePort, url), params=query,
                                    auth=HTTPBasicAuth(self.prEndpointUser, self.prEndpointPasswd))
            else:
                resp = requests.get('https://{}:{}{}'.format(self.prEndpoint, self.MInstancePort, url), params=query)
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has occured while connecting to PR endpoint with type {} at arguments {}'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(2)
        return resp.json()

    def __sr_pmds_service_query_nodes(self, cluster_uuid, **kwargs):
        valid_query_params = ["group",
                              "start",
                              "stop",
                              "node_name",
                              "field_measurement",
                              "format"]
        query_params = {k: v for (k, v) in kwargs.items() if k in valid_query_params}
        try:
            res = requests.get(f"{self.srTelemetryPMDS}/api/v1/pmds/nodes/{cluster_uuid}", params=query_params)
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has ocurred while connecting to PMDS node endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args))
            sys.exit(2)
        return res.json()

    def sr_pmds_service_query_deployments(self, cluster_uuid,
                                            namespace,
                                            **kwargs):
        valid_query_params = ["start",
                              "stop",
                              "name",
                              "format"]

        query_params = {k: v for (k, v) in kwargs.items() if k in valid_query_params}
        query_params["namespace"] = namespace
        try:
            res = requests.get(f"{self.srTelemetryPMDS}/api/v1/pmds/deployments/{cluster_uuid}", params=query_params)
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has ocurred while connecting to PMDS deployment endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args))
            sys.exit(2)
        return res

    def __sr_pmds_service_query_pods(self, cluster_uuid, namespace, **kwargs):
        valid_query_params = ["start",
                              "stop",
                              "name",
                              "node_name",
                              "format"]

        query_params = {k: v for (k, v) in kwargs.items() if k in valid_query_params}
        query_params["namespace"] = namespace

        try:
            res = requests.get(f"{self.srTelemetryPMDS}/api/v1/pmds/pods/{cluster_uuid}", params=query_params)
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has ocurred while connecting to PMDS pod endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args))
            sys.exit(2)
        return res.json()

    def cth_inventory(self, cluster_uuid):
        """
        Get cluster inventory from Serrano Central Telemetry Handler
        :param cluster_uuid:
        :return: inventory dictionary
        """
        url_inv = f"{self.central_telemetry_handler}/api/v1/telemetry/central/cluster/inventory/{cluster_uuid}"
        try:
            resp = requests.get(
                url_inv)
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has ocurred while connecting to CTH inventory endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args))
            resp = {"error": "Exception has ocurred while connecting to CTH inventory endpoint"}
        return resp

    def sr_pmds_query(self, query_param):
        '''
        Executes PMDS query in parallel using joblib backend.
        It parses the length of the arguments
        ingroups and creates a job for each group.
        It then executes the query in parallel and returns.

        :param query_param: query parameters based on PMDS API
        :return: list of responses in JSON format
        '''
        # cluster_uudi = query_param.pop('cluster_uuid')
        groups = query_param.pop('groups')
        if 'stop' in query_param:
            if not query_param['stop']:
                query_param.pop('stop')
        n_jobs = len(groups)
        querys = []
        for group in groups:
            query_param['group'] = group
            querys.append(query_param.copy())
        logger.info('[{}] : [INFO] EDE PMDS Executing parallel query with {} jobs'.format(
                datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), n_jobs))
        resp_list = Parallel(n_jobs=n_jobs, backend='threading')(
            delayed(self.__sr_pmds_service_query_nodes)(**query) for query in tqdm(querys))
        return resp_list

    def eta_status(self):
        """
        Get Enhanced telemetry agent status
        :return:
        """
        url_telem_agent = f"{self.enhanced_telemetry_agent}/api/v1/telemetry/agent"
        try:
            resp_telem_agent = requests.get(url_telem_agent)
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has ocurred while connecting to ETA status endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args))
            resp_telem_agent = {"error": "Exception has ocurred while connecting to ETA status endpoint"}
        return resp_telem_agent

    def cth_monitor(self, cluster_uuid):
        """
        Get cluster monitor from Serrano Central Telemetry Handler
        :param cluster_uuid:
        :return: monitor dictionary
        """
        url_mon = f"{self.central_telemetry_handler}/api/v1/telemetry/central/cluster/monitor/{cluster_uuid}"
        try:
            resp = requests.get(
                url_mon)
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has ocurred while connecting to CTH monitor endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args))
            resp = {"error": "Exception has ocurred while connecting to CTH monitor endpoint"}
        return resp

    def cth_metrics(self, cluster_uuid):
        """
        Get cluster metrics from Serrano Central Telemetry Handler
        :param cluster_uuid:
        :return: metrics dictionary
        """
        url_met = f"{self.central_telemetry_handler}/api/v1/telemetry/central/cluster/metrics/{cluster_uuid}"
        try:
            resp = requests.get(
                url_met)
        except Exception as inst:
            logger.error(
                '[{}] : [ERROR] Exception has ocurred while connecting to CTH metrics endpoint with type {} at arguments {}'.format(
                    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), type(inst), inst.args))
            resp = {"error": "Exception has ocurred while connecting to CTH metrics endpoint"}
        return resp
    
    def query(self,
              queryBody,
              allm=True,
              dMetrics=[],
              debug=False):
        # self.__check_valid_es()
        res = self.esInstance.search(index=self.myIndex, body=queryBody, request_timeout=230)
        if debug:
            print("%---------------------------------------------------------%")
            print("Raw JSON Ouput")
            print(res)
            print(("%d documents found" % res['hits']['total']))
            print("%---------------------------------------------------------%")
        termsList = []
        termValues = []
        ListMetrics = []
        for doc in res['hits']['hits']:
            if not allm:
                if not dMetrics:
                    sys.exit("dMetrics argument not set. Please supply valid list of metrics!")
                for met in dMetrics:
                    # prints the values of the metrics defined in the metrics list
                    if debug:
                        print("%---------------------------------------------------------%")
                        print("Parsed Output -> ES doc id, metrics, metrics values.")
                        print(("doc id %s) metric %s -> value %s" % (doc['_id'], met, doc['_source'][met])))
                        print("%---------------------------------------------------------%")
                    termsList.append(met)
                    termValues.append(doc['_source'][met])
                dictValues = dict(list(zip(termsList, termValues)))
            else:
                for terms in doc['_source']:
                    # prints the values of the metrics defined in the metrics list
                    if debug:
                        print("%---------------------------------------------------------%")
                        print("Parsed Output -> ES doc id, metrics, metrics values.")
                        print(("doc id %s) metric %s -> value %s" % (doc['_id'], terms, doc['_source'][terms])))
                        print("%---------------------------------------------------------%")
                    termsList.append(terms)
                    termValues.append(doc['_source'][terms])
                    dictValues = dict(list(zip(termsList, termValues)))
            ListMetrics.append(dictValues)
        return ListMetrics, res

    def info(self):
        # self.__check_valid_es()
        try:
            res = self.esInstance.info()
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to ES dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(2)
        return res

    def roles(self):
        # self.__check_valid_es()
        nUrl = "https://%s:%s/dmon/v1/overlord/nodes/roles" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get roles url -> %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), nUrl)
        try:
            rRoles = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(2)
        rData = rRoles.json()
        return rData

    def createIndex(self, indexName):
        # self.__check_valid_es()
        try:
            self.esInstance.create(index=indexName, ignore=400)
            logger.info('[%s] : [INFO] Created index %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), indexName)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Failed to created index %s with %s and %s',
                        datetime.fromtimestamp(time.time()).strftime(log_format), indexName, type(inst), inst.args)

    def closeIndex(self, indexName):
        try:
            self.esInstance.close(index=indexName)
            logger.info('[%s] : [INFO] Closed index %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), indexName)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Failed to close index %s with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), indexName, type(inst),
                         inst.args)

    def deleteIndex(self, indexName):
        try:
            res = self.esInstance.indices.delete(index=indexName, ignore=[400, 404])
            logger.info('[%s] : [INFO] Deleted index %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), indexName)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Failed to delete index %s with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), indexName, type(inst),
                         inst.args)
            return 0
        return res

    def openIndex(self, indexName):
        res = self.esInstance.indices.open(index=indexName)
        logger.info('[%s] : [INFO] Open index %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), indexName)
        return res

    def getIndex(self, indexName):
        res = self.esInstance.indices.get(index=indexName, human=True)
        return res

    def getIndexSettings(self, indexName):
        res = self.esInstance.indices.get_settings(index=indexName, human=True)
        return res

    def clusterHealth(self):
        res = self.esInstance.cluster.health(request_timeout=15)
        return res

    def clusterSettings(self):
        res = self.esInstance.cluster.get_settings(request_timeout=15)
        return res

    def clusterState(self):
        res = self.esInstance.cluster.stats(human=True, request_timeout=15)
        return res

    def nodeInfo(self):
        res = self.esInstance.nodes.info(request_timeout=15)
        return res

    def nodeState(self):
        res = self.esInstance.nodes.stats(request_timeout=15)
        return res

    def getStormTopology(self):
        nUrl = "https://%s:%s/dmon/v1/overlord/detect/storm" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get storm topology url -> %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), nUrl)
        try:
            rStormTopology = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            print("Can't connect to dmon at %s port %s" % (self.esEndpoint, self.dmonPort))
            sys.exit(2)
        rData = rStormTopology.json()
        return rData

    def pushAnomalyES(self, anomalyIndex, doc_type, body):
        try:
            res = self.esInstance.index(index=anomalyIndex, doc_type=doc_type, body=body)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while pushing anomaly with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(2)
        return res

    def pushAnomalyKafka(self, body):
        if self.producer is None:
            logger.warning('[{}] : [WARN] Kafka reporter not defined, skipping reporting'.format(
            datetime.fromtimestamp(time.time()).strftime(log_format)))
        else:
            try:
                self.producer.send(self.prKafkaTopic, body)
                # self.producer.flush()
                logger.info('[{}] : [INFO] Anomalies reported to kafka topic {}'.format(
                    datetime.fromtimestamp(time.time()).strftime(log_format), self.prKafkaTopic))
            except Exception as inst:
                logger.error('[{}] : [ERROR] Failed to report anomalies to kafka topic {} with {} and {}'.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), self.prKafkaTopic, type(inst), inst.args))
        return 0

    def __check_auth_pr(self):
        if self.prEndpointUser and self.prEndpointPasswd:
            return True
        elif (self.prEndpointUser is None) and (self.prEndpointPasswd is None):
            return False
        else:
            logger.error('[{}] : [ERROR] EDE Pr Endpoint auth credentials not set correctly, please check!'.format(
                datetime.fromtimestamp(time.time()).strftime(log_format), ))
            sys.exit(1)

    def getModel(self):
        return "getModel"

    def pushModel(self):
        return "push model"

    def localData(self, data):
        data_loc = os.path.join(self.dataDir, data)
        try:
            df = pd.read_csv(data_loc)
        except Exception as inst:
            logger.error('[{}] : [ERROR] Cannot load local data with  {} and {}'.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args))
            sys.exit(2)
        logger.info('[{}] : [INFO] Loading local data from {} with shape {}'.format(
            datetime.fromtimestamp(time.time()).strftime(log_format), data_loc, df.shape))
        return df

    def getInterval(self):
        nUrl = "https://%s:%s/dmon/v1/overlord/aux/interval" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get interval url -> %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), nUrl)
        try:
            rInterval = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(2)
        rData = rInterval.json()
        return rData

    def aggQuery(self, queryBody):
        adt_timeout = os.environ['ADP_TIMEOUT'] = os.getenv('ADP_TIMEOUT', str(60)) # Set timeout as env variable ADT_TIMEOUT, if not set use default 60
        # print "QueryString -> {}".format(queryBody)
        try:
            res = self.esInstance.search(index=self.myIndex, body=queryBody, request_timeout=float(adt_timeout))
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception while executing ES query with %s and %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(2)
        return res

    def getNodeList(self):
        '''
        :return: -> returns the list of registered nodes from dmon
        '''
        nUrl = "https://%s:%s/dmon/v1/observer/nodes" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get node url -> %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), nUrl)
        try:
            rdmonNode = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(2)
        rdata = rdmonNode.json()
        nodes = []
        for e in rdata['Nodes']:
            for k in e:
                nodes.append(k)
        return nodes

    def getDmonStatus(self):
        nUrl = "https://%s:%s/dmon/v1/overlord/core/status" % (self.esEndpoint, self.dmonPort)
        logger.info('[%s] : [INFO] dmon get core status url -> %s',
                    datetime.fromtimestamp(time.time()).strftime(log_format), nUrl)
        try:
            rdmonStatus = requests.get(nUrl)
        except Exception as inst:
            logger.error('[%s] : [ERROR] Exception has occured while connecting to dmon with type %s at arguments %s',
                         datetime.fromtimestamp(time.time()).strftime(log_format), type(inst), inst.args)
            sys.exit(2)
        return rdmonStatus.json()


