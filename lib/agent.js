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

var rhea = require('rhea');

var certgen = require('./certs.js');
var expose = require('./expose.js').expose;
var kubernetes = require('./kubernetes.js').client();
var log = require("./log.js").logger();
var router_deployment = require('./router_deployment.js');

const DEPLOYMENT = {
    group: 'apps',
    version: 'v1',
    name: 'deployments',
};

function succeeded(code) {
    return code >= 200 && code < 300;
}

function update_ok(code) {
    return succeeded(code) || code === 304/*not modified*/;
}

function error_handler(prefix) {
    return function (e) {
        log.error('%s: %j', prefix, e);
    };
}

function RouterDeployment(name, mode, connectors, certs) {
    this.name = name;
    kubernetes.post('services', router_deployment.client_service(this.name)).then(function () { log.info('created client service'); }).catch(error_handler('failed to create client service: '));
    certgen.tls_secret(this.name).then(function () { log.info('created client secret'); }).catch(error_handler('failed to create client secret: '));
    this.update(name, mode, connectors, certs);
}

RouterDeployment.prototype.update = function (name, mode, connectors, certs) {
    if (name !== this.name) log.error('Name change not currently supported');

    if (mode === 'edge') {
        this.router_deployment = router_deployment.edge_router_deployment(this.name, connectors);
    } else if (mode === 'interior') {
        this.router_deployment = router_deployment.interior_router_deployment(this.name, connectors);
    } else {
        log.error('Unrecognised mode: %s', mode);
        return false;
    }
    this.inter_router_secret = router_deployment.inter_router_secret(this.name, certs);
    var self = this;
    log.info('syncing secret: %s', this.inter_router_secret.metadata.name);
    return kubernetes.update('secrets', this.inter_router_secret.metadata.name, this.update_secret.bind(this)).then(function (result) {
        if (result && !update_ok(result.code)) {
            log.warn('secret %s update failed: %s', self.inter_router_secret.metadata.name.result.code);
            return Promise.reject('secret update failed: ' + result.code);
        } else {
            log.info('syncing deployment %s', self.router_deployment.metadata.name);
            return kubernetes.update(DEPLOYMENT, self.router_deployment.metadata.name, self.update_router_deployment.bind(self));
        }
    }).catch(error_handler('failed to sync: '));
};

RouterDeployment.prototype.update_router_deployment = function (actual) {
    log.info('update_router_deployment() called');
    if (actual) {
        var desired_config = router_deployment.get_router_config(this.router_deployment);
        var actual_config = router_deployment.get_router_config(actual);
        if (desired_config != actual_config) {
            router_deployment.set_router_config(actual, desired_config);
            log.info('updated deployment config');
            return actual;
        } else {
            log.info('deployment config has not changed');
            return undefined;
        }
    } else {
        log.info('no router deployment exists');
        return this.router_deployment;
    }
};

RouterDeployment.prototype.update_secret = function (actual) {
    if (actual) {
        var updated = false;
        for (var k in this.inter_router_secret.data) {
            if (this.inter_router_secret.data[k] !== actual.data[k]) {
                actual.data = this.inter_router_secret.data;
                updated = true;
            }
        }
        return updated ? actual : undefined;
    } else {
        return this.inter_router_secret;
    }
};


function Agent(name) {
    this.name = name;
    this.container = rhea.create_container({id: name});
    this.container.on('message', this.on_message.bind(this));
    this.container.on('connection_open', this.on_connection_open.bind(this));
}

Agent.prototype.connect = function () {
    this.connection = this.container.connect();
};

Agent.prototype.listen = function () {
    this.server = this.container.listen();
};

Agent.prototype.on_connection_open = function (context) {
    log.info('Subscribing for updates');
    context.connection.open_receiver('updates');
};

Agent.prototype.on_message = function (context) {
    if (context.message.subject === 'EXPOSE') {
        log.info('Got expose request');
        expose(this.router_deployment.name).then(function (hostports) {
            log.info('Exposed as %j', hostports);
            context.connection.send({subject:'EXPOSED', body:hostports});
        }).catch(function (e) {
            log.info('Failed to expose: %s', e);
            context.connection.send({subject:'EXPOSE_FAILED', body:e.toString()});
        });
    } else if (context.message.subject === 'CONFIGURE') {
        log.info('Got configure request: %s %s %j', context.message.body.name, context.message.body.mode, context.message.body.connectors);
        var config = context.message.body;
        if (this.router_deployment) {
            this.router_deployment.update(config.name, config.mode, config.connectors, config.secret);
        } else {
            this.router_deployment = new RouterDeployment(config.name, config.mode, config.connectors, config.secret);
        }
    }
};

module.exports.directed = function (name) {
    log.info('Creating directed agent: %s', name);
    var agent = new Agent(name);
    agent.connect();
    return agent;
};

module.exports.directable = function (name) {
    var agent = new Agent(name);
    agent.listen();
    return agent;
};

module.exports.standalone = function (name) {
    return certgen.generate_tls_secret(name).then(function (secret) {
        return new RouterDeployment(name, 'interior', [], secret.data);
    });
};
