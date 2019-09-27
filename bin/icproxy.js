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

function bridge_addr(config) {
    var parts = config.split(':');
    if (parts.length == 2) {
        return {
            protocol: parts[0].toLowerCase(),
            addr: parts[1]
        };
    } else {
        return undefined;
    }
}

function bridge_type(source_protocol, target_protocol) {
    return source_protocol + "_to_" + target_protocol;
}

function bridge_config(config) {
    var parts = config.split('=>');
    if (parts.length == 2) {
        var source = bridge_addr(parts[0]);
        var target = bridge_addr(parts[1]);
        if (source === undefined || target === undefined) {
            return undefined;
        } else {
            return {
                type: bridge_type(source.protocol, target.protocol),
                source: source.addr,
                target: target.addr
            };
        }
    } else {
        return undefined;
    }
}

function create_bridge(config_string) {
    var config = bridge_config(config_string);
    if (config === undefined) {
        console.error('Skipping malformed bridge: %s', config_string);
        return undefined;
    } else {
        return bridges.create(config);
    }
}

function Proxy(config, selector) {
    var targets = kubernetes.watch('pods', undefined, selector);
    var bridgeconfigs = config.split(',').map(bridge_config).filter(function (bridge) { return bridge !== undefined; });
    for (var i in bridgeconfigs) {
        var bridgeconfig = bridgeconfigs[i];
        if (bridgeconfig.type === "amqp_to_http") {
            new OutgoingBridgeConfig(bridgeconfig.source, bridgeconfig.target, bridges.amqp_to_http, targets);
        } else if (bridgeconfig.type === "amqp_to_http2") {
            new OutgoingBridgeConfig(bridgeconfig.source, bridgeconfig.target, bridges.amqp_to_http2, targets);
        } else if (bridgeconfig.type === "amqp_to_tcp") {
            new OutgoingBridgeConfig(bridgeconfig.source, bridgeconfig.target, bridges.amqp_to_tcp, targets);
        } else if (bridgeconfig.type === "http_to_amqp") {
            bridges.http_to_amqp(bridgeconfig.source, bridgeconfig.target);
        } else if (bridgeconfig.type === "http2_to_amqp") {
            bridges.http2_to_amqp(bridgeconfig.source, bridgeconfig.target);
        } else if (bridgeconfig.type === "tcp_to_amqp") {
            bridges.tcp_to_amqp(bridgeconfig.source, bridgeconfig.target);
        } else {
            console.error("Unrecognised bridge type: %s", bridgeconfig.type);
        }
    }
}

if (process.env.ICPROXY_CONFIG === undefined) {
    console.error('ICPROXY_CONFIG must be set');
} else if (process.env.ICPROXY_POD_SELECTOR === undefined) {
    console.error('ICPROXY_POD_SELECTOR must be set');
} else {
    var proxy = new Proxy(process.env.ICPROXY_CONFIG, process.env.ICPROXY_POD_SELECTOR);
}
