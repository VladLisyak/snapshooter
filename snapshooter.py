#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import json

from influxdb import InfluxDBClient

from time import localtime, strftime

import requests

requests_query = """SHOW TAG VALUES WITH KEY = "request_name" WHERE "simulation" =~ /^%(simulation)s$/"""
overview_structure = {
    "Total request count": {
        "req_count": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name"= 'allRequests' AND "simulation" =~ /^%(simulation)s$/ AND "status" = 'all' %(user_count)s %(env)s AND %(time_filter)s GROUP BY count"""
    },
    "Total failed requests count": {
        "capacity.sum {count:}": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name"= 'allRequests' AND "simulation" =~ /^%(simulation)s$/ AND "status" = 'ko' %(user_count)s %(env)s AND %(time_filter)s GROUP BY count""",
    },
    "Throughput?singlestat": {
        "capacity.mean {count: }": """SELECT MEAN(count) FROM /^%(test_type)s/ WHERE "request_name"= 'allRequests' AND "simulation" =~ /^%(simulation)s$/ AND "status" = 'all' %(user_count)s %(env)s AND %(time_filter)s GROUP BY count"""
    },
    "Min": {
        "capacity.min {min: }": """SELECT MIN(min) FROM /^%(test_type)s/ WHERE "request_name"= 'allRequests' AND "simulation" =~ /^%(simulation)s$/ AND "status" = 'all' %(user_count)s %(env)s AND %(time_filter)s GROUP BY min"""
    },
    "Median": {
        "capacity.median {mean: }": """SELECT MEDIAN(mean) FROM /^%(test_type)s/ WHERE "request_name"= 'allRequests' AND "simulation" =~ /^%(simulation)s$/ AND "status" = 'all' %(user_count)s %(env)s %(env)s AND %(time_filter)s GROUP BY mean"""
    },
    "95th percentile": {
        "capacity.percentile {percentile: }": """SELECT PERCENTILE("mean",95) FROM /^%(test_type)s/ WHERE "request_name"= 'allRequests' AND "simulation" =~ /^%(simulation)s$/ AND "status" = 'all' %(user_count)s %(env)s AND %(time_filter)s GROUP BY percentile"""
    },
    "Response Times over time ($req_status)": {
        "all requests %(calculation)s": """SELECT max("%(calculation)s") FROM /^%(test_type)s/ WHERE "simulation" =~ /^%(simulation)s$/ AND "status" =~ /^%(req_status)s$/ %(user_count)s AND "request_name" = 'allRequests' %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(none)""",
        "active users": """SELECT max("active") FROM "users" WHERE "test_type" =~ /^%(test_type)s/ AND "simulation" =~ /^%(simulation)s$/ %(user_count)s %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(none)"""
    }
    ,
    "Throughput?graph": {
        "active users": """SELECT max("active") FROM "users" WHERE "test_type" =~ /^%(test_type)s/ AND "simulation" =~ /^%(simulation)s$/ %(user_count)s %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(none)""",
        "ok requests": """SELECT max("count") FROM /^%(test_type)s/ WHERE "simulation" =~ /^%(simulation)s$/ AND "status" = 'ok' %(user_count)s AND "request_name" = 'allRequests' %(env)s AND %(time_filter)s GROUP BY time(1m) fill(none)""",
        "ko requests": """SELECT max("count") FROM /^%(test_type)s/ WHERE "simulation" =~ /^%(simulation)s$/ AND "status" = 'ko' %(user_count)s AND "request_name" = 'allRequests' %(env)s AND %(time_filter)s GROUP BY time(1m) fill(none)"""
    },
    "Response time distribution?graph": {
        "t < %(low_limit)s ms": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name" = 'allRequests' AND "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "status" = 'ok' AND "mean"<=%(low_limit)s %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(null)""",
        "%(low_limit)s < t < %(high_limit)s": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name" = 'allRequests' AND "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "status" = 'ok' AND "mean">%(low_limit)s AND "mean"<%(high_limit)s %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(null)""",
        "t > %(high_limit)s ms": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name" = 'allRequests' AND "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "status" = 'ok' AND "mean">=%(high_limit)s %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(null)""",
        "failed": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name" = 'allRequests' AND "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "status" = 'ko' %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(null)"""
    },
    "Response time distribution?mtanda-histogram-panel": {
        "response time 95 percentiles": """SELECT "percentiles95" FROM %(test_perc_name)s WHERE "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "request_name" != 'allRequests' AND "status" = '%(req_status)s' %(env)s AND %(time_filter)s"""
    }
}

separate_requests_dict = {
    "response times": {
        "%(req_name)s 95 percentile": """SELECT max("percentiles95") FROM /^%(test_type)s/ WHERE "simulation" =~ /^%(simulation)s$/ AND "status" =~ /^%(req_status)s$/ %(user_count)s AND "request_name" =~ /^%(req_name)s$/ %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(none)"""
    },
    "throughput": {
        "%(req_name)s ok requests": """SELECT max("count") FROM /^%(test_type)s/ WHERE "simulation" =~ /^%(simulation)s$/ AND "status" = 'ok' %(user_count)s AND "request_name" =~ /^%(req_name)s$/ %(env)s AND %(time_filter)s GROUP BY time(1m) fill(none)""",
        "%(req_name)s ko requests": """SELECT max("count") FROM /^%(test_type)s/ WHERE "simulation" =~ /^%(simulation)s$/ AND "status" = 'ko' %(user_count)s AND "request_name" =~ /^%(req_name)s$/ %(env)s AND %(time_filter)s GROUP BY time(1m) fill(none)""",
        "all requests": """SELECT max("count") FROM /^%(test_type)s/ WHERE "simulation" =~ /^%(simulation)s$/ AND "status" = 'all' %(user_count)s AND "request_name" = 'allRequests' %(env)s AND %(time_filter)s GROUP BY time(1m) fill(none)"""
    },
    "distribution": {
        "t < %(low_limit)s ms": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name"=~ /^%(req_name)s$/ AND "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "status" = 'ok' AND "mean"<=%(low_limit)s %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(null)""",
        "%(low_limit)s < t < %(high_limit)s": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name"=~ /^%(req_name)s$/ AND "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "status" = 'ok' AND "mean">%(low_limit)s AND "mean"<%(high_limit)s %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(null)""",
        "t > %(high_limit)s ms": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name"=~ /^%(req_name)s$/ AND "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "status" = 'ok' AND "mean">=%(high_limit)s %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(null)""",
        "failed": """SELECT SUM(count) FROM /^%(test_type)s/ WHERE "request_name"=~ /^%(req_name)s$/ AND "simulation" =~ /^%(simulation)s$/ %(user_count)s AND "status" = 'ko' %(env)s AND %(time_filter)s GROUP BY time(%(interval)s) fill(null)"""
    }
}

connection_params = {
    'host': '10.192.122.105',
    'port': 7777,
    'login': '',
    'password': '',
    'database': 'perftest'
}

arguments_dict = {
    "calculation": ["percentiles95"],
    "interval": "10s",
    "low_limit": ["2000"],
    "high_limit": ["3000"],
    "from_time": 1490958176081,
    "to_time": 1490981035437,
    "test_type": ["fix"],
    "request_name": ["big_article", "content_ref_bin"],
    "simulation": "content",
    "user_count": ["1150", "1200"],
    "env": ["papi-2-r53-stag-build-pib", "platformapis-prod"],
    "req_status": ["all"]
}


class DataProvider:
    def __init__(self, connection_params, arguments_dict):
        self.arguments_dict = arguments_dict
        self.client = InfluxDBClient(connection_params['host'], connection_params['port'], connection_params['login'],
                                     connection_params['password'], connection_params['database'])

    def get_overview_data(self, dashboard_requests_dict, query_values_dict):
        """Collect test data for overall dashboards"""
        results_dict = {}
        for name, value in dashboard_requests_dict.iteritems():
            formatted_name = name % query_values_dict
            results_dict[formatted_name] = {}
            for k, v in value.iteritems():
                results_dict[formatted_name][k % query_values_dict] = self.get_influx_data(v % query_values_dict)
        return results_dict

    def get_requests_data(self, separate_requests_dict, query_values_dict):
        """Collect test data for separate requests dashboards"""
        separate_results_dict = {}
        for name in self.arguments_dict['request_names']:
            separate_results_dict[name] = {}
            for k, v in separate_requests_dict.iteritems():
                separate_results_dict[name][k] = {}
                for query_key, query_value in v.iteritems():
                    query_values_dict['req_name'] = name
                    result = self.get_influx_data(query_value % query_values_dict)
                    separate_results_dict[name][k][query_key % query_values_dict] = result
        return separate_results_dict

    def get_influx_data(self, query, swap=True, index=0):
        """Make provided request to InfluxDB and swap datapoints for sending to Grafana"""
        resp = self.client.query(query, epoch='ms')
        values = []
        if 'series' in resp.raw:
            values = resp.raw['series'][index]['values']
            if swap:
                return self.swap_datapoints(values)
        return values

    @staticmethod
    def swap_datapoints(series_data):
        """Swap datapoints in provided datapoints array"""
        for point in series_data:
            point[0], point[1] = point[1], point[0]
        return series_data


class ArgumentsPreprocessor:
    def __init__(self, data_provider, arguments_dict, requests_query):
        self.requests_query = requests_query
        self.arguments_dict = arguments_dict
        self.data_provider = data_provider

    @staticmethod
    def remove_existing_datasets(provided_dataset, target_dataset):
        for k, v in provided_dataset.iteritems():
            if v in target_dataset[k]:
                del target_dataset[k][v]

    def process_dataset(self, dataset, ):
        "Perform all needed processing for initial arguments"
        self.process_arrays(dataset)
        self.process_multiple_types_case(dataset)
        self.process_requests(dataset)

        return dataset

    def process_requests(self, dataset):
        """Retrieve endpoints for detailed dashboards"""
        if "request_name" not in dataset \
                or (isinstance(dataset['request_name'], str) and (dataset['request_name'].lower() == "all")) \
                or isinstance(dataset['request_name'], list) and len(dataset['request_name']) == 0:
            dataset['request_name'] = "All"
            dataset['request_names'] = [item[0] for item in
                                        self.data_provider.get_influx_data(self.requests_query % self.arguments_dict)
                                        if item[0] != "allRequests"]
        else:
            dataset['request_names'] = dataset['request_name'] if isinstance(dataset['request_name'], list) \
                else [dataset['request_name']]

    @staticmethod
    def process_multiple_or_empty(element_name, initial_dict):
        """Process arguments with multiple values to insert into query"""
        if element_name == "test_type":
            if isinstance(initial_dict[element_name], list):
                if len(initial_dict[element_name]) > 1:
                    initial_dict[element_name] = "(" + "|".join(initial_dict[element_name]) + ")"
                elif len(initial_dict[element_name]) == 1:
                    initial_dict[element_name] = initial_dict[element_name][0]
                else:
                    initial_dict[element_name] = ".*"

        elif isinstance(initial_dict[element_name], str):
            initial_dict[element_name] = 'AND "%s" =~ /^%s$/' % (
                element_name, initial_dict[element_name])
        elif (isinstance(initial_dict[element_name], list) and len(initial_dict[element_name]) >= 1):
            initial_dict[element_name] = 'AND "%s" =~ /^%s$/' % (
                element_name, "(" + "|".join(initial_dict[element_name]) + ")")
        elif isinstance(initial_dict[element_name], list) and len(initial_dict[element_name]) == 0:
            initial_dict[element_name] = ""

    @staticmethod
    def process_arrays(arguments_dict):
        """Process single values received in array"""
        for k, v in arguments_dict.iteritems():
            if isinstance(v, list) and len(v) == 1:
                arguments_dict[k] = v[0]

    @staticmethod
    def process_multiple_types_case(arguments_dict):  # TODO handle multiple tests
        """Process case of multiple test types"""
        if isinstance(arguments_dict['test_type'], list):
            print "Multiple test types selected. 95th percentiles table will not be generated"
            arguments_dict['test_perc_name'] = 'Not_existing_influx_key'
        else:
            arguments_dict['test_perc_name'] = arguments_dict['test_type']


class SnapshotCreator:
    id_tracker = 0
    time_filter = "time > {:d}ms and time < {:d}ms"

    def __init__(self, connection_params, arguments_dict, overview_structure, separate_requests_structure,
                 requests_query='SHOW TAG VALUES WITH KEY = "request_name" WHERE "simulation" =~ /^%(simulation)s$/',
                 measurements_query="SHOW MEASUREMENTS", template_path="template.json"):
        arguments_dict['time_filter'] = self.time_filter.format(arguments_dict['from_time'], arguments_dict['to_time'])
        self.measurements_query = measurements_query
        self.overview_queries = overview_structure
        self.detailed_queries = separate_requests_structure
        self.connection_params = connection_params
        self.dashboard_template = self.get_dashboard_template(template_path)
        self.insert_timerange(self.dashboard_template, arguments_dict['from_time'],
                              arguments_dict['to_time'])
        self.detailed_data_dashboard = self.get_detailed_template(self.dashboard_template)
        self.data_provider = DataProvider(connection_params, arguments_dict)
        self.preprocessor = ArgumentsPreprocessor(self.data_provider, arguments_dict, requests_query)
        self.query_values_dict = dict(self.get_dashboard_keys(self.dashboard_template),
                                      **self.preprocessor.process_dataset(arguments_dict))
        self.replace_keys(self.dashboard_template, self.query_values_dict)

        self.preprocessor.process_multiple_or_empty('test_type', self.query_values_dict)
        self.preprocessor.process_multiple_or_empty('user_count', self.query_values_dict)
        self.preprocessor.process_multiple_or_empty('env', self.query_values_dict)

    def inc_and_get(self):
        """Inrement and get value for dashboard id"""
        self.id_tracker += 1
        return self.id_tracker

    def replace_ids(self, dashboard_template):
        """Replace ids in dashboard for ids consistency"""
        dashboard_template['dashboard']['id'] = self.inc_and_get()
        for row in dashboard_template['dashboard']['rows']:
            for panel in row['panels']:
                panel['id'] = self.inc_and_get()

    @staticmethod
    def insert_timerange(dashboard_body, time_from, time_to):
        """Parse timerange from arguments and insert timerange to dashboard"""
        formatted_from = strftime("%Y-%m-%dT%H:%M:%S.Z", localtime(time_from / 1000.0))
        formatted_to = strftime("%Y-%m-%dT%H:%M:%S.Z", localtime(time_to / 1000.0))
        dashboard_body['dashboard']['time']['from'] = formatted_from
        dashboard_body['dashboard']['time']['to'] = formatted_to
        dashboard_body['dashboard']['time']['raw'] = {'from': formatted_from, 'to': formatted_to}

    @staticmethod
    def get_detailed_template(dashboard_template):
        """Store separate request template separately and remove it from overall template"""
        single_dashboard = None
        for index, row in enumerate(dashboard_template['dashboard']['rows']):
            if row['title'] == 'Detailed results for $request_name':
                single_dashboard = row
                del dashboard_template['dashboard']['rows'][index]
        single_dashboard['repeat'] = None
        for item in single_dashboard['panels']:
            item['target'] = []
        return single_dashboard

    @staticmethod
    def get_dashboard_keys(template):
        """Extract from template keys that was not specified in arguments list"""
        keys_dict = {}
        for row in template['dashboard']['templating']['list']:
            keys_dict[row['name']] = row['current']['value']
        return keys_dict

    def replace_keys(self, dashboard_template, query_values_dict):
        """Replace keys in template to values specified in arguments"""
        for key in dashboard_template['dashboard']['templating']['list']:
            template_key = key["name"]
            if template_key in query_values_dict:
                name = query_values_dict[template_key]
                if isinstance(query_values_dict[template_key], list):
                    if len(query_values_dict[template_key]) > 1:
                        name = " + ".join(query_values_dict[template_key])
                    elif len(query_values_dict[template_key]) == 0:
                        name = "All"

                if name == "All" or name == ".*":
                    if template_key == "test_type":
                        template_value = [item[0] for item in
                                          self.data_provider.get_influx_data(self.measurements_query, False)]
                    else:
                        name = "All"
                        template_value = ["$__all"]
                else:
                    template_value = query_values_dict[template_key] if isinstance(query_values_dict[template_key],
                                                                                   list) else [
                        query_values_dict[template_key]]
                key["current"]["value"] = template_value
                key["current"]["text"] = name

                key["options"] = [key['current']]

    def insert_separate_dashboards(self, dataset):
        """Insert dashboards with detailed data for each separate request"""
        for result_key, result_value in dataset.iteritems():
            detailed_data_template = copy.deepcopy(self.detailed_data_dashboard)
            detailed_data_template['title'] = detailed_data_template['title'].replace("$request_name", result_key)

            for panel in detailed_data_template["panels"]:
                if 'aliasColors' in panel:
                    self.replace_color_rules(panel)
                panel['title'] = panel['title'].replace("$request_name", result_key)
                panel['id'] = self.inc_and_get()
                panel['scopedVars']['request_name']['text'] = result_key
                panel['scopedVars']['request_name']['value'] = result_key

                if ("%s response times over time ($req_status)" % result_key) == panel['title']:
                    panel['snapshotData'] = []
                    for k, v in result_value["response times"].iteritems():
                        panel['snapshotData'].append({"datapoints": v, "target": k})

                if ("%s throughput" % result_key) == panel['title']:
                    panel['snapshotData'] = []
                    for k, v in result_value["throughput"].iteritems():
                        panel['snapshotData'].append({"datapoints": v, "target": k})

                if ("%s response times distribution" % result_key) == panel['title']:
                    panel['snapshotData'] = []
                    for k, v in result_value["distribution"].iteritems():
                        panel['snapshotData'].append({"datapoints": v, "target": k})
            self.dashboard_template['dashboard']['rows'].append(detailed_data_template)

    def insert_dataset(self, dataset):
        """Insert data into overview dashboards, also insert current dashboard id"""
        for k, v in dataset.iteritems():
            dashboard_pointers = k.split("?")
            for row in self.dashboard_template['dashboard']['rows']:
                for panel in row['panels']:
                    if 'aliasColors' in panel:
                        self.replace_color_rules(panel)
                    if panel['title'] == dashboard_pointers[0]:
                        if (len(dashboard_pointers) > 1 and dashboard_pointers[1] == panel["type"]) or (
                                    len(dashboard_pointers) == 1):
                            panel['snapshotData'] = []
                            for request, datapoints in v.iteritems():
                                panel['snapshotData'].append({"datapoints": datapoints, "target": request})

    def get_snapshot(self,
                     key="Bearer eyJrIjoiQXh4eTd5eGo4cTBzNnJHYnY1NWJuaTVTd1I5eTczZE0iLCJuIjoic25hcHNob3Rfa2V5IiwiaWQiOjF9"):
        """Insert data to dashboards and make snapshot request"""
        try:
            overview_data = self.data_provider.get_overview_data(self.overview_queries, self.query_values_dict)
            separate_requests_data = self.data_provider.get_requests_data(self.detailed_queries, self.query_values_dict)

            self.insert_dataset(overview_data)
            self.insert_separate_dashboards(separate_requests_data)
            snapshot = requests.post('http://%s/api/snapshots' % self.connection_params['host'], json=self.dashboard_template, verify=False,
                                     headers=
                                     {
                                         "Accept": "application/json",
                                         "Content-type": "application/json",
                                         "Authorization": key
                                     }
                                     )

            return snapshot
        except Exception as e:
            print "Snapshot could not be created. Details: %s" % e.message

    def get_dashboard_template(self, template_path):
        """Load dashboard template from path and replace ids"""
        with open(template_path) as data_file:
            dashboard_template = json.load(data_file)
        self.replace_ids(dashboard_template)
        return dashboard_template

    def replace_color_rules(self, panel):
        """Raplace color rules in template"""
        for alias in panel["aliasColors"].keys():
            colors = panel["aliasColors"][alias]
            del panel["aliasColors"][alias]
            panel["aliasColors"][alias % self.query_values_dict] = colors


creator = SnapshotCreator(connection_params, arguments_dict, overview_structure, separate_requests_dict)

print creator.get_snapshot().json()
