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

function Proxy(config, bridgehost) {
    console.log("Proxying %s to %s", config, bridgehost);
    var bridgeconfigs = config.split(',').map(bridge_config).filter(function (bridge) { return bridge !== undefined; });
    for (var i in bridgeconfigs) {
        var bridgeconfig = bridgeconfigs[i];
        if (bridgeconfig.type === "amqp_to_http") {
            bridges.amqp_to_http(bridgeconfig.source, bridgehost, bridgeconfig.target)
        } else if (bridgeconfig.type === "amqp_to_http2") {
            bridges.amqp_to_http2(bridgeconfig.source, bridgehost, bridgeconfig.target)
        } else if (bridgeconfig.type === "amqp_to_tcp") {
            bridges.amqp_to_tcp(bridgeconfig.source, bridgehost, bridgeconfig.target)
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

var bridgehost = 'localhost'
if (process.env.ICPROXY_BRIDGE_HOST !== undefined) {
    bridgehost = process.env.ICPROXY_BRIDGE_HOST
}

var proxy = new Proxy(process.argv[2], bridgehost);
