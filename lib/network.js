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

const EventEmitter = require('events');
const util = require('util');
var certs = require("./certs.js");
var exposed_as = require("./expose.js").exposed_as;
var kubernetes = require('./kubernetes.js').client();
var log = require("./log.js").logger();
var router_deployment = require("./router_deployment.js");
var myutils = require("./utils.js");

function Network(name) {
    this.name = name  || 'icproxy-ca';
    this.issuer = certs.issuer(this.name);
    this.emitter = new EventEmitter();
    this.routers = {};
    this.pending = [];
}

Network.prototype.router_cert = function (router) {
    var args = [router.name];
    if (connectors_defined(router)) {
        args.push([router.inter_router_connector.host, router.edge_connector.host]);
    }
    return this.issuer.generate_tls_secret.apply(this.issuer, args).then(function (secret) {
        router.secret = secret;
        changed(router);
    });
};

Network.prototype.agent_cert = function (name) {
    return this.issuer.generate_tls_secret(name, undefined, this.connect_info)
};

Network.prototype.director_cert = function () {
    return this.issuer.generate_tls_secret('icproxy-director', [this.connect_info.host]);
};

var network = new Network();

function added(router) {
    network.emitter.emit('added', router);
}

function deleted(router) {
    router.emit('deleted', router);
    network.emitter.emit('deleted', router);
}

function changed(router) {
    router.emit('changed', router);
    network.emitter.emit('changed', router);
}

function connector_function (mode) {
    if (mode === 'interior') return function (target) { return target.inter_router_connector; };
    else if (mode === 'edge') return function (target) { return target.edge_connector; };
    else throw new Error('Invalid mode: ' + mode);
}

function Router (name, mode) {
    EventEmitter.call(this);
    this.name = name;
    this.mode = mode;
    this.connected_to = {};
    //following are only valid when mode === 'interior'
    this.edge_connector = undefined;
    this.inter_router_connector = undefined;
    this.connected_from = {};
}

util.inherits(Router, EventEmitter);

Router.prototype.configure = function () {
    return {
        name: this.name,
        mode: this.mode,
        connectors: Object.keys(this.connected_to).map(lookup).filter(connectors_defined).map(connector_function(this.mode)),
        secret: this.secret.data
    };
};

Router.prototype.expose = function () {
    return this.is_interior() && !connectors_defined(this);
};

function connectors_defined (router) {
    return router.inter_router_connector && router.edge_connector;
};

Router.prototype.connect = function (router) {
    if (!router.is_interior()) {
        throw new Error('Cannot connect to ' + to + '; not an interior router');
    }
    this.connected_to[router.name] = router.name;
    router.connected_from[this.name] = this.name;
    changed(this);
};

Router.prototype.disconnect = function (router) {
    if (!router.is_interior()) {
        throw new Error('Not an interior router: ' + name);
    }
    delete this.connected_to[router.name];
    delete router.connected_from[this.name];
    changed(this);
};

Router.prototype.disconnect_all = function () {
    this.connected_to.map(lookup).forEach(this.disconnect.bind(this));
};

Router.prototype.is_interior = function () {
    return this.mode === 'interior';
};

Router.prototype.is_edge = function () {
    return this.mode === 'edge';
};

Router.prototype.disconnect_clients = function () {
    var keys = Object.keys(this.connected_from);
    keys.forEach(function (key) {
        network.routers[key].disconnect(this);
    });
    changed(this);
};

Router.prototype.to_edge = function () {
    if (!router.is_interior()) {
        throw new Error('Cannot convert ' + this.name + '; not an interior router');
    }
    this.mode = 'edge';
    this.disconnect_clients();
};

Router.prototype.to_interior = function () {
    if (!router.is_edge()) {
        throw new Error('Cannot convert ' + this.name + '; not an edge router');
    }
    this.mode = 'interior';
    changed(this);
};

Router.prototype.exposed = function (inter_router_connector, edge_connector) {
    this.inter_router_connector = inter_router_connector;
    this.edge_connector = edge_connector;
    log.info('%s exposed; regenerating certificate...', this.name);
    //generate new certificate, then notify all routers connected to us they need to reconfigure
    var self = this;
    return network.router_cert(this).then(function () {
        log.info('Certificate regenerated for %s; notifying connectees', self.name);
        Object.keys(self.connected_from).map(lookup).forEach(changed);
    });
};

function edge_router(name) {
    return new Router(name, 'edge');
}

function interior_router(name) {
    return new Router(name, 'interior');
}

function lookup (name, routers) {
    var index = routers || network.routers;
    var router = index[name];
    if (router === undefined) {
        throw new Error('No such router: ' + name);
    } else if (router.name !== name) {
        throw new Error('Index error: ' + name + ' -> ' + router.name);
    }
    return router;
};

function ensure_bootstrap(name) {
    var configname = name + '-bootstrap';
    return kubernetes.get('configmaps', configname).catch(function (code) {
        if (code === 404) {
            log.info('Generating bootstrap configmap %s', configname);
            return network.agent_cert(name).then(function (secret) {
                secret.metadata.name += '-agent';
                return kubernetes.post('configmaps', router_deployment.bootstrap_config(configname, secret)).then(function () {
                    log.info('Generated bootstrap configmap %s', configname);
                });
            });
        } else {
            log.error('Error getting bootstrap configmap %s: %s', configname, code);
            return Promise.reject('Error getting bootstrap configmap ' + configname + ': ' + code);
        }
    });
}

function add(router) {
    if (!network.routers[router.name] || !network.routers[router.name].secret) {
        network.pending.push(network.router_cert(router).then(function () {
            network.routers[router.name] = router;
            log.info('generated cert for router %s', router.name);
            added(router);
            //now generate cert and boostrap yaml for agent if needed:
            return ensure_bootstrap(router.name);
        }));
    }
}

function interior (name) {
    var router = network.routers[name];
    if (router === undefined) {
        router = interior_router(name);
        add(router);
    } else if (router.is_edge()) {
        router.to_interior();
    } else if (router.name !== name) {
        throw new Error('Index error: ' + name + ' -> ' + router.name);
    }
    return router;
};

function edge (name) {
    var router = network.routers[name];
    if (router === undefined) {
        router = edge_router(name);
        add(router);
    } else if (router.is_interior()) {
        router.to_edge();
    } else if (router.name !== name) {
        throw new Error('Index error: ' + name + ' -> ' + router.name);
    }
    return router;
};

function remove(name) {
    var router = lookup(name);
    if (router.is_interior) {
        router.disconnect_clients();
    }
    router.disconnect_all();
    delete network.routers[name];
    deleted(router);
}

function connect (from, to, index) {
    lookup(from, index).connect(lookup(to, index));
};

module.exports.lookup = lookup;

module.exports.init = function () {
    return exposed_as(process.env.DIRECTOR_NAME || 'icproxy-director').then(function (connect_info) {
        network.connect_info = connect_info;
        log.info('connect info for director: %j', network.connect_info);
        return network.director_cert();
    });
};

module.exports.read = function (def) {
    /*
     * Expected format:
     * ----------------
     *
     * Router AWS some-test-url.aws.com
     * EdgeRouter PVT
     * Connect PVT AWS
     *
     */
    network.pending = [];
    // Routers aren't added to the proper index until their certs have
    // been generated, which is done asynchronously. A temporary index
    // is maintained during the update process to allow the
    // connections to be marked.
    var routers = {};
    var lines = def.trim().split(/\s*[\r\n]+\s*/g);
    lines.forEach(function (line) {
        var parts = line.split(/\s+/);
        if (parts[0] === 'Router' && parts.length >= 2) {
            var router = interior(parts[1]);
            routers[router.name] = router;
            //TODO: handle url(s) if provided
        } else if (parts[0] === 'EdgeRouter' && parts.length === 2) {
            var router = edge(parts[1]);
            routers[router.name] = router;
        } else if (parts[0] === 'Connect' && parts.length === 3) {
            connect(parts[1], parts[2], routers);
        } else {
            log.warn('Skipping line in network definition, could not parse: %s', line);
        }
    });
    // Detect deleted routers:
    Object.keys(myutils.difference(network.routers, routers)).forEach(remove);
    // TODO: detect removed connections

    // Allow caller to determine when triggerred activity has settled:
    return Promise.all(network.pending).then(function () {
        network.pending = [];
    });
};
