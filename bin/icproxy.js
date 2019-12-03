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

var bridges = require('../lib/bridges.js');
var kubernetes = require('../lib/kubernetes.js').client();

function is_pod_running(pod) {
    return pod.status.phase === 'Running';
}

function is_pod_ready(pod) {
    for (var i in pod.status.conditions) {
        if (pod.status.conditions[i].type === 'Ready') {
            return pod.status.conditions[i].status === 'True';
        }
    }
}

function is_pod_running_and_ready(pod) {
    return is_pod_running(pod) && is_pod_ready(pod);
}

function get_pod_ip(pod) {
    return pod.status.podIP;
}

function OutgoingBridgeConfig(address, target_port, factory, targets) {
    this.address = address;
    this.target_port = target_port;
    this.factory = factory;
    this.bridges = {};
    targets.on('updated', this.updated.bind(this));
}

OutgoingBridgeConfig.prototype.updated = function (pods) {
    var hosts = pods.filter(is_pod_running_and_ready).map(get_pod_ip);
    var removed = {};
    for (var host in this.bridges) {
        removed[host] = true;
    }
    for (var i in hosts) {
        var host = hosts[i];
        if (this.bridges[host] === undefined) {
            this.bridges[host] = this.factory(this.address, host, this.target_port);
        }
        delete removed[host];
    }
    for (var host in removed) {
        if (this.bridges[host]) {
            console.log('Stopping bridge for %s', host);
            this.bridges[host].stop();
            delete this.bridges[host];
        } else {
            console.log('Could not stop bridge for %s; no bridge found', host);
        }
    }
};

function get_bridging_functions(protocol) {
    if (protocol === 'http') {
        return {
            ingress: bridges.http_to_amqp,
            egress: bridges.amqp_to_http
        };
    } else if (protocol === 'tcp') {
        return {
            ingress: bridges.tcp_to_amqp,
            egress: bridges.amqp_to_tcp
        };
    } else if (protocol === 'http2') {
        return {
            ingress: bridges.http2_to_amqp,
            egress: bridges.amqp_to_http2
        };
    } else {
        return undefined;
    }
}

function get_ordinal(hostname) {
    var matches = hostname.match(/\d+$/);
    if (matches) {
        return parseInt(matches[0]);
    } else {
        log.error('Could not determine ordinal for %s', hostname);
        return 0;
    }
}

function get_target_pod_name(hostname, statefulset) {
    return statefulset + '-' + get_ordinal(hostname);
}

function Proxy(config) {
    var bridging = get_bridging_functions(config.protocol);
    if (bridging) {
        if (config.headless === undefined) {
            var address = config.address || config.name;
            this.ingress = bridging.ingress(config.port, address);

            if (config.targets) {
                this.egress = config.targets.map(function (target) {
                    if (target.selector) {
                        var targets = kubernetes.watch('pods', undefined, target.selector);
                        return new OutgoingBridgeConfig(address, target.targetPort || config.port, bridging.egress, targets);
                    } else {
                        console.error('Ignoring target %s; no selector defined.', target.name);
                        return {};
                    }
                });
            }
        } else {
            //for headless services, only have ingress on remote sites, and only have egress on local site
            if (config.origin) {
                var address = [config.address || config.name, process.env.HOSTNAME].join('.');
                this.ingress = bridging.ingress(config.port, address);
            } else {
                var podname = get_target_pod_name(process.env.HOSTNAME, config.headless.name);
                var address = [config.address || config.name, podname].join('.');
                var targetPort = config.headless.targetPort || config.port;
                var host = [podname, config.name || config.address, process.env.NAMESPACE, 'svc.cluster.local'].join('.');
                this.egress = bridging.egress(address, host, targetPort);
            }
        }
    } else {
        console.error('Unrecognised protocol: %s', config.protocol);
    }
}

process.on('SIGTERM', function () {
    console.log('Exiting due to SIGTERM');
    process.exit();
});

if (process.env.SKUPPER_PROXY_CONFIG === undefined) {
    console.error('SKUPPER_PROXY_CONFIG must be set');
} else {
    var proxy = new Proxy(JSON.parse(process.env.SKUPPER_PROXY_CONFIG));
}
