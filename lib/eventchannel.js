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
var util = require('util');
var rhea = require('rhea');
var url_parse = require('url').parse;

var log = require('./log.js').logger();
var metrics = require('./metrics.js');
var client_ip = require('./utils.js').client_ip;

function elapsed(start) {
    return Date.now() - start;
}

function AmqpToHttpBridge(address, host, port) {
    log.info('Created AMQP to HTTP bridge %s => %s:%s', address, host, port);
    this.host = host;
    this.port = port;
    this.address = address;
    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_amqp:' + this.address + '=>http://' + this.host  + ':' + this.port;
    this.connection = container.connect();
    this.connection.on('message', this.incoming.bind(this));
    this.connection.open_receiver({source:address, autoaccept:true,credit_window:10/*TODO: make configurable*/});
}

AmqpToHttpBridge.prototype.incoming = function (context) {
    var start = Date.now();
    var url = url_parse('http://' + context.message.to);
    var headers = {};
    for (var key in context.message.application_properties) {
        if (key === 'skupper-path') {
            url = url_parse('http://' + context.message.application_properties[key]);
        } else {
            headers[key] = context.message.application_properties[key];
        }
    }
    if (context.message.content_type) {
        headers['content-type'] = context.message.content_type;
    }
    var options = {
        host: this.host,
        port: this.port,
        path: url.path,
        method: context.message.subject,
        headers: headers
    };
    var self = this;
    var request = http.request(options, function (response) {
        log.info('%s: %s', url, response.statusCode);
	response.on('end', function () {
            metrics.http_egress_request(self.metrics, self.host, options.method, response.statusCode, context.message.body.length, 0, elapsed(start));
        });
    });
    request.on('error', function (error) {
        console.error(error);
    });
    request.write(context.message.body);
    request.end();
};

AmqpToHttpBridge.prototype.stop = function () {
    this.connection.close();
};


function bind(client, event) {
    client.connection.on(event, client['on_' + event].bind(client));
}

function SyncSender (connection, target) {
    this.requests = [];
    this.handlers = {};
    this.connection = connection;
    this.target = target;
    //use anonymous link so that we always have credit, but still get
    //acknowledged message transfer
    this.sender = this.connection.open_sender({target:{}});
    bind(this, 'sendable');
    bind(this, 'disconnected');
    bind(this, 'settled');
};

SyncSender.prototype.on_sendable = function (context) {
    this.send_pending_requests();
};

SyncSender.prototype.on_settled = function (context) {
    var handler = this.handlers[context.delivery.id];
    if (handler) {
        handler.resolve();
        delete this.handlers[context.delivery.id];
    }
};

SyncSender.prototype.send = function (request) {
    var delivery = this.sender.send(request.message);
    this.handlers[delivery.id] = request.handler;
};

SyncSender.prototype.send_pending_requests = function (context) {
    while (this.ready() && this.requests.length > 0) {
        this.send(this.requests.shift());
    }
    return this.requests.length === 0;
};

SyncSender.prototype.send_or_queue = function (request) {
    if (this.ready() && this.send_pending_requests()) {
        this.send(request);
    } else {
        this.requests.push(request);
    }
};

SyncSender.prototype.request = function (message) {
    var self = this;
    return new Promise(function (resolve, reject) {
        self.send_or_queue({message: message, handler: {resolve:resolve, reject:reject}});
    });
};

SyncSender.prototype.ready = function (details) {
    return this.sender.sendable();
};

SyncSender.prototype.close = function () {
    this.connection.close();
};

SyncSender.prototype.on_disconnected = function (context) {
    for (var key in this.handlers) {
        this.handlers[key].reject('aborted');
    }
    this.handlers = {};
};

function HttpToAmqpBridge(port, address) {
    log.info('Created HTTP to AMQP bridge %s => %s', port, address);
    this.port = port;
    this.address = address;
    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_http:' + this.port + '=>amqp:' + address;
    this.sender = new SyncSender(container.connect(), address);

    //listen for http requests
    this.server = http.createServer(this.request.bind(this));
    this.server.listen(this.port, '0.0.0.0');
    log.info('listening for http on %s', this.port);
}

function append(a, b) {
    if (a === undefined) return b;
    else return Buffer.concat([a, b]);
}

HttpToAmqpBridge.prototype.request = function (request, response) {
    if (request.method !== 'POST') {
        response.statusCode = 405;
        response.end(util.format('%s not allowed on %s', request.method, request.url));
        return;
    }

    var start = Date.now();
    var self = this;
    var url = url_parse(request.url);
    var path = url.pathname;
    var address = request.headers.host ? request.headers.host.split(':')[0] + path : request.url;
    log.info('outgoing request %s (%s)', request.headers.host, address);

    var message_out = {
        to: this.address,
        subject: request.method,
        application_properties: {'skupper-path':address},
        message_annotations: {},
        body: undefined
    };
    for (var key in request.headers) {
        if (key === 'content-type') {
            message_out.content_type = request.headers[key];
        } else {
            message_out.application_properties[key] = request.headers[key];
        }
    }
    request.on('data', function (chunk) { message_out.body = append(message_out.body, chunk); });
    request.on('end', function () {
        log.info('posting event to %s', message_out.to);
        self.sender.request(message_out).then(function () {
            response.statusCode = 200;
            response.end();
            metrics.http_ingress_request(self.metrics, client_ip(request), request.method, response.statusCode, message_out.body.length, 0, elapsed(start));
        }).catch(function (error) {
            console.error(error);
            response.statusCode = 500;
            response.end(error.toString());
            metrics.http_ingress_request(self.metrics, client_ip(request), request.method, response.statusCode, message_out.body.length, 0, elapsed(start));
        });
    });
};

module.exports.amqp_to_http = function (address, host, port) {
    return new AmqpToHttpBridge('mc/'+address, host, port);
};

module.exports.http_to_amqp = function (port, address) {
    return new HttpToAmqpBridge(port, 'mc/'+address);
};
