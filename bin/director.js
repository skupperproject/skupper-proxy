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
var certs = require('../lib/certs.js');
var kubernetes = require('../lib/kubernetes.js').client();
var log = require('../lib/log.js').logger();
var network = require('../lib/network.js');
var myutils = require('../lib/utils.js');

var issuer = certs.issuer('dummyca');
var secrets = {};
var agents = {};
var routers = {};
var edge_connectors = [];
var server;

function ConnectedAgent (id, connection, sender, router) {
    this.id = id;
    this.connection = connection;
    this.sender = sender;
    this.router = router;
    this.router.on('changed', this.configure.bind(this));
    this.connection.on('disconnected', this.disconnected.bind(this));
}

ConnectedAgent.prototype.configure = function (sender) {
    this.sender.send({subject:'CONFIGURE', body:this.router.configure()});
};

ConnectedAgent.prototype.disconnected = function () {
    this.router.removeListener('changed', this.configure.bind(this));
};

//TODO: move some of this to network
//TODO: watch network definition configmap for changes:
kubernetes.get('configmaps', 'icproxy-network').then(function (configmap) {
    network.init().then( function (secret) {
        network.read(configmap.data.network).then(function () {
            server = rhea.listen({
                port:5671,
                transport: 'tls',
                requestCert: true,
                rejectUnauthorized: true,
                key: myutils.base64decode(secret.data['tls.key']),
                cert: myutils.base64decode(secret.data['tls.crt']),
                ca: myutils.base64decode(secret.data['ca.crt'])
            });
            server.on('listening', function () {
                log.info('listening on %s', server.address().port);
            });
        }).catch(function (e) {
            log.error('Error reading network definition: %s', e);
        });
    }).catch(function (e) {
        log.error('Error initialising: %s', e);
    });
});

function get_id(connection) {
    var cert = connection.get_peer_certificate();
    if (cert && cert.subject) return cert.subject.CN;
    else log.error('Could not retrieve certificate CN for connection, falling back to container id');
}

rhea.on('connection_open', function (context) {
    var id = get_id(context.connection);
    if (id === undefined) {
        context.connection.close({ condition: 'amqp:internal-error', description: 'Could not retrieve CN' });
    } else {
        log.info('agent connected: %s', id);
    }
});

rhea.on('sender_open', function (context) {
    var id = get_id(context.connection);
    if (id !== undefined) {
        log.info('agent subscribed: %s', id);
        var router;
        try {
            router = network.lookup(id);
        } catch (e) {
            log.error('Could not lookup %s: %s', id, e);
            context.connection.close({ condition: 'amqp:connection:forced', description: e.toString() });
        }

        if (router) {
            var agent = new ConnectedAgent(id, context.connection, context.sender, router);
            agents[id] = agent;
            agent.configure();
            if (router.expose()) {
                context.sender.send({subject:'EXPOSE'});
            }
        }
    }
});

rhea.on('disconnected', function (context) {
    var id = get_id(context.connection);
    if (id) {
        log.info('agent disconnected: %s', id);
        delete agents[id];
    }
});

rhea.on('message', function (context) {
    if (context.message.subject === 'EXPOSED') {
        var agent = agents[get_id(context.connection)]
        log.info('%s now exposed %j', context.connection.container_id, context.message.body);
        agent.router.exposed(context.message.body['inter-router'], context.message.body['edge']);
    } else if (context.message.subject === 'EXPOSE_FAILED') {
        log.error(context.message.body);
    }
});
