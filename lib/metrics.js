/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

var http = require('http');
var fs = require('fs');
var path = require('path');
var url = require('url');
var util = require('util');
var rhea = require('rhea');
var kubernetes = require('./kubernetes.js').client();
var log = require('./log.js').logger();
var qdr = require('./qdr.js');
var myutils = require('./utils.js');


function http_request_metrics() {
    return {
        requests: 0,
        bytes_in: 0,
        bytes_out: 0,
        details: {},//request count keyed by method+response_code
        latency_max: 0//TODO: add percentiles
    };
}

function record_http_request (metrics, method, response_code, bytes_in, bytes_out, latency) {
    metrics.requests++;
    metrics.bytes_in += bytes_in;
    metrics.bytes_out += bytes_out;
    if (latency > metrics.latency_max) {
        metrics.latency_max = latency;
    }
    var key = method + ':' + response_code;
    if (metrics.details[key] === undefined) {
        metrics.details[key] = 1;
    } else {
        metrics.details[key]++;
    }
};

function record_indexed_http_request (index, key, method, response_code, bytes_in, bytes_out, latency) {
    var metrics = index[key];
    if (metrics === undefined) {
        metrics = http_request_metrics();
        index[key] = metrics;
    }
    record_http_request(metrics, method, response_code, bytes_in, bytes_out, latency);
    return metrics;
}

function merge_counts(a, b) {
    for (var k in b) {
        if (a[k]) {
            a[k] += b[k];
        } else {
            a[k] = b[k];
        }
    }
    return a;
}

function merge_http_request_metrics(a, b) {
    a.requests += b.requests;
    a.bytes_in += b.bytes_in;
    a.bytes_out += b.bytes_out;
    merge_counts(a.details, b.details);
    //TODO: add support for percentiles
    if (b.latency_max > a.latency_max) {
        a.latency_max = b.latency_max;
    }
    return a;
}

function merged_indexed_http_request_metrics (a, b) {
    if (a === undefined) return b;
    if (b === undefined) return a;
    for (var k in b) {
        if (a[k]) {
            merge_http_request_metrics(a[k], b[k]);
        } else {
            a[k] = b[k];
        }
    }
    return a;
}

function http_ingress() {
    var metrics = http_request_metrics();
    metrics.by_client = {};
    return metrics;
}

function http_egress() {
    var metrics = http_request_metrics();
    metrics.by_server = {};
    metrics.by_originating_site = {};
    return metrics;
}

function http_metrics (address) {
    return {
        address: address,
        protocol: 'http',
        ingress: http_ingress(),
        egress: http_egress()
    };
}

function http2_metrics (address) {
    var result = http_metrics(address);
    result = 'http2';
    return result;
}

function BridgeMetricsAgent (origin, metrics) {
    this.origin = origin
    this.container = rhea.create_container({id:'bridge-metrics-' + origin, enable_sasl_external:true})
    this.connection = this.container.connect();
    this.connection.open_receiver("mc/$"+origin);
    this.sender = this.connection.open_sender({target:{}});
    this.connection.on('message', this.on_message.bind(this));
    this.metrics = metrics;
}

BridgeMetricsAgent.prototype.on_message = function (context) {
    log.info('bridge metrics agent received %j', context.message);
    if (context.message.subject === 'bridge-metrics-request') {
        this.sender.send({to: context.message.reply_to, correlation_id:context.message.correlation_id, subject: 'bridge-metrics', group_id: process.env.HOSTNAME, body: JSON.stringify(this.metrics)});
        log.info('bridge metrics agent sent reply');
    }
}

function translate_keys(map, lookup) {
    var results = {};
    for (var k in map) {
        var k2 = lookup[k];
        if (k2) {
            results[k2] = map[k];
        } else {
            results[k] = map[k];
        }
    }
    log.info('translate_keys(%j, %j) => %j', map, lookup, results);
    return results;
}

function translate_object_for_key(map, key, lookup) {
    for (var k in map) {
        var v2 = lookup[map[k][key]];
        if (v2) {
            map[k][key] = v2;
        }
    }
}

function set(entries) {
    var items = {};
    var count = 0;
    entries.forEach(function (e) {
        items[e] = e;
        count++;
    });
    return {
        add: function (item) {
            if (items[item] !== undefined) {
                items[item] = item;
                count++;
                return true;
            } else {
                return false;
            }
        },
        remove: function (item) {
            if (items[item] !== undefined) {
                delete items[item];
                count--;
                return true;
            } else {
                return false;
            }
        },
        empty: function () {
            return count === 0;
        },
        has: function (item) {
            return items[item] !== undefined;
        },
        items: function () {
            return Object.keys(items);
        }
    };
}

function AggregatedBridgeMetrics(proxies, ip_lookup, resolve, reject) {
    this.expected = set(proxies);
    this.ip_lookup = ip_lookup;
    this.resolve = resolve;
    this.reject = reject;
    this.metrics = {};
}

AggregatedBridgeMetrics.prototype.is_complete = function () {
    return this.expected.empty();
};

AggregatedBridgeMetrics.prototype.add_metrics = function (source, added) {
    log.info('handling bridge metrics from %s: %j', source, added);
    if (this.expected.remove(source)) {
        if (added.protocol === 'http') {
            this.add_http_metrics(added);
        } else if (added.protocol === 'tcp') {
            this.add_tcp_metrics(added);
            log.info('AggregatedBridgeMetrics.add_metrics(%s, %j): %j', source, added, this.metrics);
        }
        if (this.expected.empty()) {
            log.info('have received all expected bridge-metrics');
            this.resolve(this.metrics);
        } else {
            log.info('still waiting for bridge-metrics from %j', this.expected.items());
        }
    } else {
        log.info('duplicate bridge metrics from %s', source);
    }
};

AggregatedBridgeMetrics.prototype.add_tcp_metrics = function (added) {
    translate_object_for_key(added.ingress, 'client', this.ip_lookup);
    translate_object_for_key(added.egress, 'server', this.ip_lookup);
    var current = this.metrics[added.address];
    if (current === undefined) {
        current = {
            protocol: added.protocol,
            connections_ingress: {},
            connections_egress: {}
        };
        this.metrics[added.address] = current;
    }
    current.connections_ingress = myutils.merge(current.connections_ingress, added.ingress);
    current.connections_egress = myutils.merge(current.connections_egress, added.egress);
};

AggregatedBridgeMetrics.prototype.add_http_metrics = function (added) {
    var by_client = translate_keys(added.ingress.by_client, this.ip_lookup);
    var by_server = translate_keys(added.egress.by_server, this.ip_lookup);
    var current = this.metrics[added.address];
    if (current === undefined) {
        this.metrics[added.address] = {
            protocol: added.protocol,
            requests_received: {
                requests: added.ingress.requests,
                bytes_in: added.ingress.bytes_in,
                bytes_out: added.ingress.bytes_out,
                details: added.ingress.details,
                by_client: by_client
            },
            requests_handled: {
                requests: added.egress.requests,
                bytes_in: added.egress.bytes_in,
                bytes_out: added.egress.bytes_out,
                details: added.egress.details,
                by_server: by_server,
                by_originating_site: added.egress.by_originating_site
            },
        };
    } else {
        // have more than one proxy pod in local site serving the
        // same address
        current.requests_received.requests += added.ingress.requests;
        current.requests_received.bytes_in += added.ingress.bytes_in;
        current.requests_received.bytes_out += added.ingress.bytes_out;
        merge_counts(current.requests_received.details, added.ingress.details);

        current.requests_handled.requests += added.egress.requests;
        current.requests_handled.bytes_in += added.egress.bytes_in;
        current.requests_handled.bytes_out += added.egress.bytes_out;
        merge_counts(current.requests_handled.details, added.egress.details);
        merged_indexed_http_request_metrics(current.requests_received.by_client, by_client);
        merged_indexed_http_request_metrics(current.requests_handled.by_server, by_server);
        merged_indexed_http_request_metrics(current.requests_handled.by_originating_site, by_originating_site);
    }
};

function get_site_name(name) {
    var end = name.indexOf('-skupper-router');
    return end > 0 ? name.substring(0, end) : name;
}

function get_values(map) {
    var result = [];
    for (var key in map) {
        result.push(map[key]);
    }
    return result;
}

function AggregatedSiteMetrics(sites, resolve, reject) {
    this.expected = set(sites);
    this.resolve = resolve;
    this.reject = reject;
    this.sites = [];
    this.services = {};
    this.metrics = {};
}

AggregatedSiteMetrics.prototype.is_complete = function () {
    return this.expected.empty();
};

AggregatedSiteMetrics.prototype.add_metrics = function (source, added) {
    if (this.expected.remove(source)) {
        //aggregate the results
        //site_name, site_id, bridge_metrics (which is keyed on address)
        //want to combine by address

        //sites
        this.sites.push({
            site_name: added.site_name ? get_site_name(added.site_name) : '',
            site_id: added.site_id,
            connected: added.connected_sites,
            namespace: added.namespace,
            url: added.url,
            edge: added.edge
        });
        //services
        for (var address in added.bridge_metrics) {
            var service = this.services[address];
            var _service = added.bridge_metrics[address];
            if (service === undefined) {
                service = {
                    address: address,
                    protocol: _service.protocol,
                    targets: []
                };
                if (service.protocol === 'http') {
                    service.requests_received = [];
                    service.requests_handled = [];
                } else if (service.protocol === 'tcp') {
                    service.connections_ingress = [];
                    service.connections_egress = [];
                }
                this.services[address] = service;
            } else {
                if (service.protocol != _service.protocol) {
                    log.error('protocol mismatch for %s, have %s but site %s has %s', address, service.protocol, added.site_id, _service.protocol);
                }
            }
            if (service.protocol === 'http') {
                if (_service.requests_received.requests > 0) {
                    var received = {
                        site_id: added.site_id,
                        by_client: _service.requests_received.by_client
                    };
                    service.requests_received.push(received);
                }

                if (_service.requests_handled.requests > 0) {
                    var handled = {
                        site_id: added.site_id,
                        by_server: _service.requests_handled.by_server,
                        by_originating_site: _service.requests_handled.by_originating_site
                    };
                    service.requests_handled.push(handled);
                }
            } else if (service.protocol === 'tcp') {
                log.info('Adding tcp stats %j to %j', _service, service);
                if (Object.keys(_service.connections_ingress).length > 0) {
                    service.connections_ingress.push({
                        site_id: added.site_id,
                        connections: _service.connections_ingress
                    });
                }
                if (Object.keys(_service.connections_egress).length > 0) {
                    service.connections_egress.push({
                        site_id: added.site_id,
                        connections: _service.connections_egress
                    });
                }
            }

            var targets = added.targets[address];
            if (targets) {
                targets.forEach(function (target) {
                    service.targets.push({
                        name: target.name,
                        target: target.target,
                        site_id: added.site_id
                    });
                });
            } else {
                log.info('No target for %s in %j', address, added.targets);
            }
        }

        if (this.expected.empty()) {
            this.resolve({
                sites: this.sites,
                services: get_values(this.services)
            });
        } else {
            log.info('still waiting for site-metrics from %j', this.expected.items());
        }
    } else {
        log.info('duplicate site metrics from %s', source);
    }
};

function pod_ready (pod) {
    for (var i in pod.status.conditions) {
        if (pod.status.conditions[i].type === 'Ready') {
            return pod.status.conditions[i].status === 'True';
        }
    }
    return false;
}

function pod_running (pod) {
    return pod.status.phase === 'Running';
}

function pod_ready_and_running (pod) {
    return pod_ready(pod) && pod_running(pod);
}

function is_proxy_pod (pod) {
    //TODO: change to use internal.skupper.io/type === proxy
    return pod.metadata.labels && pod.metadata.labels['internal.skupper.io/service'] != undefined;
}

function pod_summary (pod) {
    return {
        name: pod.metadata.name,
        ip: pod.status.podIP,
        labels: pod.metadata.labels
    };
}

function get_pod_name (pod) {
    return pod.metadata.name;
}

function equivalent_pod (a, b) {
    return a.name === b.name && a.ip === b.ip;
}

function name_compare (a, b) {
    return myutils.string_compare(a.name, b.name);
}

function parse_selector (selector) {
    var elements = selector.split(',');
    var labels = {};
    for (var i in elements) {
        var parts = elements[i].split('=');
        labels[parts[0]] = parts[1] || null;
    }
    return function (podsummary) {
        for (var key in labels) {
            if (!(podsummary.labels[key] && (labels[key] === null || labels[key] === podsummary.labels[key]))) {
                log.debug('pod with labels %j does not match selector %j on %s', podsummary.labels, labels, key);
                return false;
            }
        }
        return true;
    };
}

function get_matcher (service) {
    var matchers = service.targets ? service.targets.map(function (target) {
        return {
            name: target.name,
            match: parse_selector(target.selector)
        }
    }) : [];
    return function (pod, matched) {
        if (matchers.length === 0) {
            log.info('no targets to match against for %j', pod.name, service);
        }
        for (var i in matchers) {
            if (matchers[i].match(pod)) {
                matched.push({
                    name: pod.name,
                    target: matchers[i].name
                });
                log.info('pod %s matched %j', pod.name, service);
            } else {
                log.info('pod %s did not match %j', pod.name, service);
            }
        }
    };
}

function SiteMetricsAgent(origin) {
    this.config_watcher = kubernetes.watch_resource('configmaps', 'skupper-services');
    this.config_watcher.on('updated', this.service_definitions_updated.bind(this));
    this.service_mapping = undefined; // pod name -> list of services (and target names) of which that pod is a target

    this.pod_watcher = kubernetes.watch('pods');
    this.pod_watcher.on('updated', this.pods_updated.bind(this));
    this.ip_lookup = {}; // ip -> name
    this.current_pods = []; // summary of all pods in namespace, used to maintain ip_lookup and service_mapping
    this.proxies = []; // list of the proxy pods (used to determine when all bridge results have been received)

    this.origin = origin;
    this.container = rhea.create_container({id:'metrics-collector' + origin, enable_sasl_external:true});
    this.connection = this.container.connect();
    this.connection.open_receiver({source:{dynamic:true}}).on('receiver_open', this.on_receiver_open.bind(this));
    this.local_sender = this.connection.open_sender("mc/$"+origin);
    this.connection.open_receiver("mc/$controllers");
    this.global_sender = this.connection.open_sender("mc/$controllers");
    this.connection.on('message', this.on_message.bind(this));
    this.address = undefined;
    this.pending = [];

    this.ready = [];
    this.counter = 1;
    this.inprogress_bridge_metrics_requests = {};
    this.inprogress_site_metrics_requests = {};

    this.router = new qdr.Router(this.container.connect());
    var self = this;
    this.connection.on('connection_open', function (context) {
        self.site_id = context.connection.container_id;//TODO: replace with something more robust
    });
}

SiteMetricsAgent.prototype.service_definitions_updated = function (configmaps) {
    var definitions = {};
    if (configmaps && configmaps.length == 1) {
        for (var name in configmaps[0].data) {
            try {
                var record = JSON.parse(configmaps[0].data[name]);
                record.match = get_matcher(record);
                definitions[name] = record;
            } catch (e) {
                log.error('Could not parse service definition for %s as JSON: e', name, e);
            }
        }
        log.info('Got service definitions: %j', definitions);
    } else {
        log.info('No service definition configmap yet exists');
    }
    this.services = definitions;
    this.build_service_mapping();
};

SiteMetricsAgent.prototype.build_service_mapping = function () {
    if (this.services && this.current_pods) {
        var mapping = {};//service to list of pods
        for (var address in this.services) {
            var listing = [];
            var service = this.services[address];
            for (var i in this.current_pods) {
                service.match(this.current_pods[i], listing);
            }
            if (listing.length) {
                mapping[address] = listing;
            }
        }
        this.service_mapping = mapping;
        log.info('built service mapping: %j', mapping);
        this.check_ready();
    } else {
        if (!this.services) log.info('need services to be defined before mapping can be built');
        if (!this.current_pods) log.info('need pods to be retrieved before mapping can be built');
    }
};

SiteMetricsAgent.prototype.wait_until_ready = function () {
    var self = this;
    return new Promise(function (resolve, reject) {
        if (self.address === undefined || self.service_mapping === undefined) {
            log.info('Waiting for metrics agent to be ready');
            self.ready.push(resolve);
        } else {
            log.info('Metrics agent is ready');
            resolve();
        }
    });
};

SiteMetricsAgent.prototype.check_ready = function () {
    if (this.service_mapping && this.address) {
        if (this.ready.length) {
            log.info('Metrics agent now ready');
            while (this.ready.length) {
                this.ready.pop()();
            }
        }
    } else if (this.ready) {
        log.info('Metrics agent not yet ready');
        if (!this.service_mapping) log.info('waiting for service mapping');
        if (!this.address) log.info('waiting for reply address');
    }
};

SiteMetricsAgent.prototype.pods_updated = function (pods) {
    this.proxies = pods.filter(pod_ready_and_running).filter(is_proxy_pod).map(get_pod_name);
    var latest = pods.map(pod_summary);
    latest.sort(name_compare);
    var changes = myutils.changes(this.current_pods, latest, name_compare, equivalent_pod);
    if (changes) {
        this.current_pods = latest;
        var ip_lookup = this.ip_lookup;
        changes.added.forEach(function (pod) {
            if (pod.ip) ip_lookup[pod.ip] = pod.name;
        });
        changes.removed.forEach(function (pod) {
            delete ip_lookup[pod.ip];
        });
        changes.modified.forEach(function (pod) {
            if (pod.ip) ip_lookup[pod.ip] = pod.name;
            else delete ip_lookup[pod.ip];
        });
    }
    if (this.service_mapping === undefined) {
        this.build_service_mapping();
    }
};

SiteMetricsAgent.prototype.on_receiver_open = function (context) {
    this.address = context.receiver.source.address;
    this.check_ready();
};

SiteMetricsAgent.prototype.on_message = function (context) {
    log.info('site metrics agent received %j', context.message);
    if (context.message.subject === 'bridge-metrics') {
        var request = this.inprogress_bridge_metrics_requests[context.message.correlation_id];
        if (request) {
            request.add_metrics(context.message.group_id, JSON.parse(context.message.body));
            log.info('site metrics agent handled bridge-metrics response from %s', context.message.group_id);
            if (request.is_complete()) {
                delete this.inprogress_bridge_metrics_requests[context.message.correlation_id];
            }
        } else {
            log.error('unexpected bridge-metrics response from %s (%s)', context.message.group_id, context.message.correlation_id);
        }
    } else if (context.message.subject === 'site-metrics') {
        var request = this.inprogress_site_metrics_requests[context.message.correlation_id];
        if (request) {
            request.add_metrics(context.message.group_id, JSON.parse(context.message.body));
            log.info('site metrics agent handled site-metrics response from %s', context.message.group_id);
            if (request.is_complete()) {
                delete this.inprogress_site_metrics_requests[context.message.correlation_id];
            }
        } else {
            log.error('unexpected site-metrics response from %s (%s)', context.message.group_id, context.message.correlation_id);
        }
    } else if (context.message.subject === 'site-metrics-request') {
        var self = this;
        this.wait_until_ready().then(function () {
            self.get_site_metadata().then(function (metadata) {
                log.info('sites metadata: %j', metadata);
                self.collect_proxy_metrics().then(function (results) {
                    var site_metrics = myutils.merge({
                        site_name: self.site_id,
                        site_id: self.origin,
                        targets: self.service_mapping,
                        bridge_metrics: results
                    }, metadata);
                    context.connection.send({to:context.message.reply_to, group_id:self.origin, correlation_id:context.message.correlation_id, subject:'site-metrics', body:JSON.stringify(site_metrics)});
                    log.info('site metrics agent sent reply %j', site_metrics);
                }).catch(function (error) {
                    log.error(error);
                    context.connection.send({to:context.message.reply_to, group_id:self.origin, correlation_id:context.message.correlation_id, subject:'site-metrics', body:JSON.stringify(error)});
                });
            }).catch(function (error) {
                log.error(error);
                context.connection.send({to:context.message.reply_to, group_id:self.origin, correlation_id:context.message.correlation_id, subject:'site-metrics', body:JSON.stringify(error)});
            });
        }).catch(function (error) {
            log.error(error);
            context.connection.send({to:context.message.reply_to, group_id:self.origin, correlation_id:context.message.correlation_id, subject:'site-metrics', body:JSON.stringify(error)});
        });
    }
}

SiteMetricsAgent.prototype.next_request_id = function () {
    return 'request-' + this.counter++;
};

SiteMetricsAgent.prototype.collect_proxy_metrics = function () {
    if (this.proxies.length === 0) {
        log.warn('No proxies found to collect metrics from');
        return Promise.resolve({});
    }

    //send out request, wait for responses
    var request_id = this.next_request_id();
    this.local_sender.send({subject: 'bridge-metrics-request', reply_to:this.address, correlation_id:request_id});
    log.info('site metrics agent sent bridge-metrics-request');
    //wait for all the results
    var self = this;
    return new Promise(function (resolve, reject) {
        self.inprogress_bridge_metrics_requests[request_id] = new AggregatedBridgeMetrics(self.proxies, self.ip_lookup, resolve, reject);
    });
};

SiteMetricsAgent.prototype.collect = function () {
    var self = this;
    return this.wait_until_ready().then(function () {
        log.info('retrieving all site ids');
        return self.get_site_ids().then(function (ids) {
            log.info('collecting site metrics for %j', ids);
            if (ids.length === 0) {
                return Promise.resolve({});
            } else {
                //send out request, wait for responses
                var request_id = self.next_request_id();
                self.global_sender.send({subject: 'site-metrics-request', reply_to:self.address, correlation_id:request_id});
                log.info('site metrics agent sent site-metrics-request');
                return new Promise(function (resolve, reject) {
                    self.inprogress_site_metrics_requests[request_id] = new AggregatedSiteMetrics(ids, resolve, reject);
                });
            }
        });
    });
};

function get_all_edge_ids(router, router_ids) {
    return Promise.all(router_ids.map(function (id) {
        var agent = router.get_router_agent_for_id(id);
        return agent.get_connections().then(function (connections) {
            return connections.filter(is_attached_edge).map(get_container);
        });
    })).then(function (results) {
        return [].concat.apply([], results);
    });
}

function get_all_interior_ids(router) {
    return router.get_nodes().then(function (nodes) {
        return nodes.map(function (node) {
            return node.id;
        });
    });
}

function get_site_ids_from_router_ids(router, ids) {
    log.info('get_site_ids_from_router_ids(%j)', ids)
    return Promise.all(ids.map(function (id) {
        return router.get_router_for_id(id.id, id.type === 'edge').then(function (r) {
            log.info('retrieved router for %j: %j', id, r);
            return r[0].metadata;
        });
    }));
}

function annotate_with_type(type) {
    return function (id) {
        return {
            id: id,
            type: type
        };
    }
}

function get_all_site_ids_in_network(interior) {
    return get_all_interior_ids(interior).then(function (interior_ids) {
        log.info('interior routers: %j', interior_ids);
        return get_all_edge_ids(interior, interior_ids).then(function (edge_ids) {
            log.info('edge routers: %j', edge_ids);
            return get_site_ids_from_router_ids(interior, interior_ids.map(annotate_with_type('interior')).concat(edge_ids.map(annotate_with_type('edge'))));
        });
    });
}

SiteMetricsAgent.prototype.get_site_ids = function () {
    var router = this.router;
    return router.query('org.apache.qpid.dispatch.router').then(function (rdetails) {
        if (rdetails[0].mode === 'edge') {
            log.info('local site is edge');
            //need to get the router we are uplinked to, then get the nodes from that
            return router.get_connections().then(get_edge_uplink_connections).then(function (edges) {
                if (edges.length === 0) {
                    //edge router without an edge uplink, i.e. no other siteids
                    return [rdetails[0].metadata];
                } else {
                    log.info('local uplink is to %s', edges[0].container);
                    return get_all_site_ids_in_network(router.get_router_agent_for_id(edges[0].container));
                }
            });
        } else {
            log.info('local site is interior');
            return get_all_site_ids_in_network(router);
        }
    });
};

function is_edge_connection(connection) {
    return connection.role === 'edge';
}

function is_inter_router_connection(connection) {
    return connection.role === 'inter-router';
}

function is_inter_router_outgoing(connection) {
    return is_inter_router_connection(connection) && connection.dir === 'out';
}

function is_edge_uplink(connection) {
    return is_edge_connection(connection) && connection.dir === 'out';
}

function is_attached_edge(connection) {
    return is_edge_connection(connection) && connection.dir === 'in';
}

function get_edge_uplink_connections(connections) {
    return connections.filter(is_edge_uplink);
}

function get_outgoing_inter_router_connections(connections) {
    return connections.filter(is_inter_router_outgoing);
}

function get_container(connection) {
    return connection.container;
}

SiteMetricsAgent.prototype.get_connected_to_sites = function () {
    var router = this.router;
    return router.query('org.apache.qpid.dispatch.router').then(function (rdetails) {
        if (rdetails[0].mode === 'edge') {
            log.info('Getting edge uplink...');
            //only connected site is the router we are uplinked to
            return router.get_connections().then(get_edge_uplink_connections).then(function (edges) {
                log.info('Got edge connections: %j', edges);
                if (edges.length === 0) {
                    return [];
                } else {
                    return router.get_router_for_id(edges[0].container).then(function (results) {
                        log.info('Got connected routers: %j', results);
                        return [results[0].metadata];
                    });
                }
            });
        } else {
            log.info('Getting all outgoing router connections for interior...');
            return router.get_connections().then(get_outgoing_inter_router_connections).then(function (conns) {
                return Promise.all(
                    conns.map(function (conn) {
                        log.info('Getting router details for %s', conn.container);
                        //need to get the metadata field for the router identified by this id
                        return router.get_router_for_id(conn.container);
                    })
                ).then(function (results) {
                    log.info('Got connected routers: %j', results);
                    return results.map(function (routers) {
                        return routers[0].metadata;
                    });
                });
            });
        }
    });
};

SiteMetricsAgent.prototype.get_connected_sites = function () {
    var router = this.router;
    return router.query('org.apache.qpid.dispatch.router').then(function (rdetails) {
        if (rdetails.mode === 'edge') {
            //only connected site is the router we are uplinked to
            return router.get_connections().then(get_edge_uplink_connections).then(function (edges) {
                if (edges.length === 0) {
                    return [];
                } else {
                    return [edges[0].metadata];
                }
            });
        } else {
            return router.get_nodes().then(function (nodes) {
                return Promise.all(
                    nodes.filter(function (node) {
                        return !node.nextHop;
                    }).map(function (node) {
                        //need to get the metadata field for the router identified by this id
                        return router.get_router_for_id(node.id);
                    })
                ).then(function (results) {
                    log.info('Got connected routers: %j', results);
                    return results.map(function (routers) {
                        return routers[0].metadata;
                    });
                });
            });
        }
    });
};

const ROUTE_DIVIDER = '.apps.';

SiteMetricsAgent.prototype.get_site_url = function () {
    return kubernetes.get(kubernetes.ROUTE, 'skupper-inter-router').then(function (route) {
        if (route.spec.host) {
            return route.spec.host.substring(route.spec.host.indexOf(ROUTE_DIVIDER) + ROUTE_DIVIDER.length);
        } else {
            log.error('Could not get host for route: %j', route);
            return undefined;
        }
    }).catch(function (code, error) {
        log.warn('Could not retrieve inter router route: %j %j', code, error);
        return kubernetes.get('services', 'skupper-internal').then(function (service) {
            if (service.spec.type === 'LoadBalancer') {
                log.info('skupper-internal loadbalancer service status: %j', service.status);
                return service.status.loadBalancer.ingress[0].ip || service.status.loadBalancer.ingress[0].hostname;
            } else {
                log.info('skupper-internal is not a load-balancer service (%s)', service.spec.type);
                return undefined;
            }
        }).catch(function (code, error) {
            log.warn('Could not retrieve internal service: %j %j', code, error);
            return undefined;
        });
    });
};

SiteMetricsAgent.prototype.get_kubernetes_namespace = function () {
    return new Promise(function(resolve, reject) {
        fs.readFile('/var/run/secrets/kubernetes.io/serviceaccount/namespace', 'utf8', function (err, data) {
            if (err) {
                console.log('Could not retrieve namespace: %s', err);
                resolve(undefined);
            } else {
                resolve(data);
            }
        });
    });
}

SiteMetricsAgent.prototype.is_edge_site = function () {
    var self = this;
    return this.router.query('org.apache.qpid.dispatch.router').then(function (router) {
        return router[0].mode === 'edge';
    });
};

SiteMetricsAgent.prototype.get_site_metadata = function () {
    var self = this;
    return Promise.all([
        this.get_connected_to_sites(),
        this.is_edge_site(),
        this.get_site_url(),
        this.get_kubernetes_namespace()
    ]).then(function (results) {
        return {
            connected_sites: results[0],
            edge: results[1],
            url: results[2],
            namespace: results[3]
        };
    });
};

function dummy_metrics(address) {
    return {address: address};
}

module.exports.create_bridge_metrics = function (address, protocol, origin) {
    var factory = dummy_metrics;
    if (protocol === 'http' || protocol === 'http2') {
        factory = http_metrics;
    } else if (protocol === 'http2') {
        factory = http2_metrics;
    } else if (protocol === 'tcp') {
        factory = tcp_metrics;
    } else {
        factory = dummy_metrics;
    }
    var agent = new BridgeMetricsAgent(origin, factory(address));
    log.info('initialised agent with metrics: %j', agent.metrics);
    return agent.metrics;
};

module.exports.http_ingress_request = function (metrics, clientip, method, response_code, site, bytes_in, bytes_out, latency) {
    if (metrics) {
        record_http_request(metrics.ingress, method, response_code, bytes_in, bytes_out, latency);
        var client = record_indexed_http_request(metrics.ingress.by_client, clientip, method, response_code, bytes_in, bytes_out, latency);
        if (client.by_handling_site === undefined) {
            client.by_handling_site = {};
        }
        if (site) {
            record_indexed_http_request(client.by_handling_site, site, method, response_code, bytes_in, bytes_out, latency);
        }
        log.info('http ingress request %j', metrics);
    }
};

module.exports.http_egress_request = function (metrics, serverpod, method, response_code, site, bytes_in, bytes_out, latency) {
    if (metrics) {
        record_http_request(metrics.egress, method, response_code, bytes_in, bytes_out, latency);
        record_indexed_http_request(metrics.egress.by_server, serverpod, method, response_code, bytes_in, bytes_out, latency);
        record_indexed_http_request(metrics.egress.by_originating_site, site, method, response_code, bytes_in, bytes_out, latency);
        log.info('http egress request %j', metrics);
    }
};

function tcp_connection(id) {
    return {
        id: id,
        start_time: Date.now(),
        bytes_in: 0,
        bytes_out: 0
    }
}

function tcp_metrics (address) {
    return {
        address: address,
        protocol: 'tcp',
        // ingress and egress are both keyed on connection id of form host:port@site
        ingress: {},
        egress: {}
    };
}

function tcp_record_connection_open (metrics, id) {
    metrics[id] = tcp_connection(id);
    return metrics[id];
};

function tcp_record_data_in (metrics, id, bytes_in) {
    var conn = metrics[id];
    if (conn) {
        conn.bytes_in += bytes_in;
        conn.last_in = Date.now();
    } else {
        log.warn('Could not record tcp data in for %s', id);
    }
};

function tcp_record_data_out (metrics, id, bytes_out) {
    var conn = metrics[id];
    if (conn) {
        conn.bytes_out += bytes_out;
        conn.last_out = Date.now();
    } else {
        log.warn('Could not record tcp data out for %s', id);
    }
};

function tcp_record_connection_close (metrics, id) {
    delete metrics[id];
};

module.exports.tcp_connection_id = function (clientip, clientport, site) {
    return util.format('%s:%s@%s', clientip, clientport, site);
};

module.exports.tcp_ingress_connection_open = function (metrics, id, client) {
    if (metrics) {
        tcp_record_connection_open(metrics.ingress, id).client = client;
        log.info('tcp ingress connection open %j', metrics);
    }
};

module.exports.tcp_ingress_data_in = function (metrics, id, bytes_in) {
    if (metrics) {
        tcp_record_data_in(metrics.ingress, id, bytes_in);
        log.info('tcp ingress data in %j', metrics);
    }
};

module.exports.tcp_ingress_data_out = function (metrics, id, bytes_out) {
    if (metrics) {
        tcp_record_data_out(metrics.ingress, id, bytes_out);
        log.info('tcp ingress data out %j', metrics);
    }
};

module.exports.tcp_ingress_connection_close = function (metrics, id) {
    if (metrics) {
        tcp_record_connection_close(metrics.ingress, id);
        log.info('tcp ingress connection close %j', metrics);
    }
};

module.exports.tcp_egress_connection_open = function (metrics, id, server) {
    if (metrics) {
        tcp_record_connection_open(metrics.egress, id).server = server;
        log.info('tcp egress connection open %j', metrics);
    } else {
        log.info('tcp egress connection open; metrics not enabled');
    }
};

module.exports.tcp_egress_data_in = function (metrics, id, bytes_in) {
    if (metrics) {
        tcp_record_data_in(metrics.egress, id, bytes_in);
        log.info('tcp egress data in %j', metrics);
    }
};

module.exports.tcp_egress_data_out = function (metrics, id, bytes_out) {
    if (metrics) {
        tcp_record_data_out(metrics.egress, id, bytes_out);
        log.info('tcp egress data out %j', metrics);
    }
};

module.exports.tcp_egress_connection_close = function (metrics, id) {
    if (metrics) {
        tcp_record_connection_close(metrics.egress, id);
        log.info('tcp egress connection close %j', metrics);
    }
};

var content_types = {
    '.html': 'text/html',
    '.js': 'text/javascript',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.jpg': 'image/jpg',
    '.gif': 'image/gif',
    '.woff': 'application/font-woff',
    '.ttf': 'application/font-ttf',
    '.eot': 'application/vnd.ms-fontobject',
    '.otf': 'application/font-otf',
    '.svg': 'image/svg+xml'
};

function get_content_type(file) {
    return content_types[path.extname(file).toLowerCase()];
}

function static_handler(request, response) {
    var file = path.join(__dirname, '../console/', url.parse(request.url).pathname);
    if (file.charAt(file.length - 1) === '/') {
        file += 'index.html';
    }
    fs.readFile(file, function (error, data) {
        if (error) {
            response.statusCode = error.code === 'ENOENT' ? 404 : 500;
            response.end(http.STATUS_CODES[response.statusCode]);
            log.warn('GET %s => %i %j', request.url, response.statusCode, error);
        } else {
            var content_type = get_content_type(file);
            if (content_type) {
                response.setHeader('content-type', content_type);
            }
            log.debug('GET %s => %s', request.url, file);
            response.end(data);
        }
    });
}

function parse_credentials(auth_header) {
    var parts = auth_header ? auth_header.split(' ') : [];
    if (parts[0] === 'Basic') {
        var credentials = Buffer.from(parts[1], 'base64').toString('ascii').split(':');
        return {
            username: credentials[0],
            password: credentials[1]
        };
    } else {
        return undefined;
    }
}

function authenticate(request, response, authf) {
    if (authf) {
        log.info('Checking credentials...');
        var credentials = parse_credentials(request.headers.authorization);
        if (credentials) {
            log.info('Have credentials %j. Authenticating...', credentials);
            return authf(credentials.username, credentials.password).then(function (result) {
                if (result) {
                    return true;
                } else {
                    response.setHeader('WWW-Authenticate', 'Basic realm=skupper');
                    response.statusCode = 401;
                    response.end('Authentication failed');
                    return false;
                }
            });
        } else {
            log.info('Authentication required');
            response.setHeader('WWW-Authenticate', 'Basic realm=skupper');
            response.statusCode = 401;
            response.end('Authentication required');
            return Promise.resolve(false);
        }
    } else {
        log.info('No authentication enabled');
        return Promise.resolve(true);
    }
}

//to be called by each controller
module.exports.create_server = function (origin, port, host, authf) {
    var agent = new SiteMetricsAgent(origin);
    var server = http.createServer(function (request, response) {
        authenticate(request, response, authf).then(function (ok) {
            if (ok) {
                if (request.method === 'GET') {
                    var u = url.parse(request.url);
                    log.info('request for %s', u.pathname);
                    if (u.pathname === '/DATA') {
                        agent.collect().then(function (results) {
                            response.statusCode = 200;
                            response.end(JSON.stringify(results, null, 2) + "\n");
                        }).catch(function (error) {
                            response.statusCode = 500;
                            response.end("Cause: " + error + "\n\n");
                        });
                    } else {
                        static_handler(request, response);
                    }
                } else {
                    response.statusCode = 405;
                    response.end(util.format('%s not allowed on %s', request.method, request.url));
                }
            }
        });
    });
    server.listen(port, host);
    return server
}
