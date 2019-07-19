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

var kubernetes = require('./kubernetes.js').client();
var log = require("./log.js").logger();
var router_deployment = require('./router_deployment.js');

function route(name, targetPort) {
    return {
        apiVersion: 'route.openshift.io/v1',
        kind: 'Route',
        metadata: {
            name: name + '-' + targetPort
        },
        spec: {
            port: {
                targetPort: targetPort
            },
            tls: {
                insecureEdgeTerminationPolicy: 'None',
                termination: 'passthrough'
            },
            to: {
                kind: 'Service',
                name: name
            }
        }
    };
}

const ROUTE = {
    group: 'route.openshift.io',
    version: 'v1',
    name: 'routes',
};

function create_route(name, targetPort) {
    return kubernetes.post(ROUTE, route(name, targetPort));
}

function create_routes(name) {
    return Promise.all([create_route(name, 'inter-router'), create_route(name, 'edge')]);
}

function get_route(name, targetPort) {
    return kubernetes.get(ROUTE, route(name, targetPort).metadata.name);
}

function get_hostports_for_routes(service_name) {
    return Promise.all([get_route(service_name, 'inter-router'), get_route(service_name, 'edge')]).then(function (results) {
        return {
            'inter-router': {
                host: results[0].spec.host,
                port: 443
            },
            'edge': {
                host: results[1].spec.host,
                port: 443
            }
        };
    });
}

function create_service_and_routes(name) {
    var service = router_deployment.inter_router_service(name);
    return kubernetes.post('services', service).then(function () {
        return create_routes(service.metadata.name).then(function () {
            return get_hostports_for_routes(service.metadata.name);
        });
    });
}

function retry (f, test, handler, stop) {
    if (Date.now() < stop) {
        return f().then(function (result) {
            if (test(result)) {
                return handler(result);
            } else {
                return retry(f, test, handler, stop);
            }
        });
    } else {
        return Promise.reject('Timed out');
    }
}

function get_loadbalancer_when_ready(service_name, timeout) {
    var end = Date.now() + timeout;
    function retrieve () {
        return kubernetes.get('services', service_name);
    }
    function test (retrieved) {
        var result = retrieved.status.loadBalancer && retrieved.status.loadBalancer.ingress && retrieved.status.loadBalancer.ingress[0].ip;
        console.log('test(%j): %s', retrieved.status, result);
        return result;
        //return retrieved.status.loadBalancer && retrieved.status.loadBalancer.ingress && retrieved.status.loadBalancer.ingress[0].ip;
    }
    function result (retrieved) {
        return retrieved;
    }
    return retry(retrieve, test, result, end);
}

function find_by_name(items, name) {
    for (var i = 0; i < items.length; i++) {
        if (name === undefined || items[i].name === name) return items[i];
    }
    return undefined;
}

function get_host_port_for_loadbalancer(lb_svc, port_name) {
    return {
        host: lb_svc.status.loadBalancer.ingress[0].host || lb_svc.status.loadBalancer.ingress[0].ip,
        port: find_by_name(lb_svc.spec.ports, port_name).nodePort
    };
}

function get_loadbalancer_host_ports(service_name, timeout) {
    return get_loadbalancer_when_ready(service_name, timeout).then(function (retrieved) {
        return {
            'inter-router': get_host_port_for_loadbalancer(retrieved, 'inter-router'),
            'edge': get_host_port_for_loadbalancer(retrieved, 'edge')
        };
    });
}

function create_loadbalancer_service(name) {
    var service = router_deployment.inter_router_service(name, 'LoadBalancer');
    console.log('creating service: %j', service);
    return kubernetes.post('services', service).then(function () {
        return get_loadbalancer_host_ports(service.metadata.name, 1000);
    });
}

function has_routes() {
    return kubernetes.list(ROUTE).then(function () {
        return true;
    }).catch(function (e) {
        log.info('Routes not supported: %s', e);
        return false;
    });
}

module.exports.expose = function (name) {
    if (process.env.NO_ROUTES) {
        return create_loadbalancer_service(name);
    } else {
        return has_routes().then(function (result) {
            if (result) return create_service_and_routes(name);
            else return create_loadbalancer_service(name);
        });
    }
};

module.exports.exposed_as = function (name) {
    return kubernetes.get(ROUTE, name).then(function (route) {
        return {
            host: route.spec.host,
            port: 443
        };
    }).catch(function (e) {
        log.info('Could not get route host for %s: %s', name, e);
        return get_loadbalancer_when_ready(name, 100).then(function(result) {
            return get_host_port_for_loadbalancer(result);
        }).catch(function (e) {
            log.warn('Could not get service ingress ip for %s: %s', name, e);
            return {
                host: 'localhost',
                port: 5671
            };
        });
    });

};
