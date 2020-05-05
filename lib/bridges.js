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
var net = require('net');
var rhea = require('rhea');
var url_parse = require('url').parse;

var log = require('./log.js').logger();
var metrics = require('./metrics.js');
var myhttp2 = require('./http2.js');
var rclient = require('./request.js');
var tunnel = require('./tunnel.js');
var client_ip = require('./utils.js').client_ip;


function append(a, b) {
    if (a === undefined) return b;
    else return Buffer.concat([a, b]);
}

function elapsed(start) {
    return Date.now() - start;
}

function AmqpToHttpBridge(address, host, port) {
    log.info('Created AMQP to HTTP bridge %s => %s:%s', address, host, port);
    this.host = host;
    this.port = port;
    this.address = address;
    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_amqp_' + this.address + '_to_http_' + this.host  + '_' + this.port;
    this.connection = container.connect();
    this.connection.on('message', this.incoming.bind(this));
    this.connection.open_receiver({source:address, autoaccept:false,credit_window:10/*TODO: make configurable*/});
}

AmqpToHttpBridge.prototype.incoming = function (context) {
    var start = Date.now();
    var url = url_parse('http://' + context.message.to);
    var headers = {};
    var site = context.message.group_id;
    for (var key in context.message.application_properties) {
        headers[key] = context.message.application_properties[key];
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
        log.info('%s: %s', url.href, response.statusCode);
        var message_out = {
            to: context.message.reply_to,
            correlation_id: context.message.correlation_id,
            subject: '' + response.statusCode,
            application_properties: {},
            group_id: self.site_id,
            body: undefined
        };
        for (var key in response.headers) {
            if (key === 'content-type') {
                message_out.content_type = response.headers['content-type'];
            } else {
                message_out.application_properties[key] = response.headers[key]
            }
        }
	response.on('data', function (chunk) { message_out.body = append(message_out.body, chunk); });
	response.on('end', function () {
            context.delivery.accept();
            log.info('server sending reply for request to %s: %j', url.href, message_out);
            context.connection.send(message_out);
            metrics.http_egress_request(self.metrics, self.host, options.method, response.statusCode, site, context.message.body ? context.message.body.length : 0, message_out.body.length, elapsed(start));
        });
    });
    request.on('error', function (error) {
        log.error('%s', error);
        context.delivery.modified();
    });
    request.on('timeout', function () {
        log.error('request to %s timed out', url.href);
        context.delivery.modified();
    });
    if (context.message.body !== null) {
        request.write(context.message.body);
    }
    request.setTimeout(60000);
    request.end();
};

AmqpToHttpBridge.prototype.stop = function () {
    this.connection.close();
};

function HttpToAmqpBridge(port, address) {
    log.info('Created HTTP to AMQP bridge %s => %s', port, address);
    this.port = port;
    this.address = address;
    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_http_' + this.port + '_to_amqp_' + address;
    this.client = rclient.create(container.connect(), address);

    //listen for http requests
    this.server = http.createServer(this.request.bind(this));
    this.server.listen(this.port, '0.0.0.0');
    log.info('listening for http on %s', this.port);
}

HttpToAmqpBridge.prototype.request = function (request, response) {
    var start = Date.now();
    var self = this;
    var url = url_parse(request.url);
    var path = url.path;
    var address = request.headers.host ? request.headers.host.split(':')[0] + path : url_parse(request.url);
    log.info('outgoing request %s (%s)', request.headers.host, address);

    var body;
    request.on('data', function (data) { body = append(body, data); });
    request.on('end', function () {
        var message_out = {
            to: address,
            subject: request.method,
            application_properties: {},
            group_id: self.site_id,
            message_annotations: {},
            body: body
        };
        for (var key in request.headers) {
            if (key === 'content-type') {
                message_out.content_type = request.headers[key];
            } else {
                message_out.application_properties[key] = request.headers[key];
            }
        }

        log.info('client sending message: %s %s', message_out.subject, message_out.to);
        self.client.request(message_out).then(function (message_in) {
            log.info('got reply for outbound request: %s %s', message_in.subject, message_in.to);
            var site = message_in.group_id;
            for (var key in message_in.application_properties) {
                response.setHeader(key, message_in.application_properties[key]);
            }
            response.statusCode = message_in.subject || 203;
            if (message_in.content_type) {
                response.setHeader('content-type', message_in.content_type);
            }
            response.end(message_in.body ? message_in.body : undefined);
            metrics.http_ingress_request(self.metrics, client_ip(request.socket), request.method, response.statusCode, site, message_out.body ? message_out.body.length : 0, message_in.body ? message_in.body.length: 0, elapsed(start));
        }).catch(function (error) {
            console.error(error);
            response.statusCode = 500;
            response.end(error.toString());
            metrics.http_ingress_request(self.metrics, client_ip(request.socket), request.method, response.statusCode, site, message_out.body ? message_out.body.length : 0, error.toString().length, elapsed(start));
        });
    });
};

function AmqpToTcpBridge(address, host, port) {
    log.info('Created AMQP to TCP bridge %s => %s:%s', address, host, port);
    this.address = address;
    this.host = host;
    this.port = port;

    this.container = rhea.create_container({enable_sasl_external:true});
    this.container.id = process.env.HOSTNAME + '_amqp_' + address + '_to_tcp_' + this.host  + '_' + this.port;
    this.connection = this.container.connect();

    this.connection.on('connection_open', this.on_connection_open.bind(this));
    this.connection.on('receiver_open', this.on_receiver_open.bind(this));
    this.mgmt_client = rclient.create(this.connection, '$management');
}

AmqpToTcpBridge.prototype.on_connection_open = function (context) {
    //send mgmt request to create link route for address
    var props = {
        'operation': 'CREATE',
        'type': 'org.apache.qpid.dispatch.router.connection.linkRoute',
        'name': this.address
    };
    var definition = {
        'pattern': this.address,
        'direction': 'in'
    };
    var self = this;
    this.mgmt_client.request({application_properties:props,body:definition}).then(function (result) {
        if (result.application_properties.statusCode >= 200 && result.application_properties.statusCode < 300) {
            log.info('[%s] created connection scoped link route', self.container.id);
        } else {
            log.error('[%s] failed to create connection scoped link route: %s [%s]', self.container.id, result.application_properties.statusDescription, result.application_properties.statusCode);
        }
    }).catch(function (error) {
        log.error('[%s] failed to create connection scoped link route: %j', self.container.id, error);
    });

};

AmqpToTcpBridge.prototype.on_receiver_open = function (context) {
    if (context.receiver === this.mgmt_client.receiver) return;
    log.info('[%s] receiver attached', this.container.id);
    var socket = net.connect({host: this.host, port: this.port});
    var self = this;
    var connected = false;
    socket.on('connect', function () {
        connected = true;
        log.info('[%s] socket connected to %s:%s', self.container.id, self.host, self.port);
        tunnel.server(context.receiver, socket, self.host, self.metrics);
    });
    socket.on('error', function (error) {
        if (!connected) {
            log.error('Failed to create server tunnel: %s', error);
            context.receiver.close();
        }
    });
};

AmqpToTcpBridge.prototype.stop = function () {
    this.connection.close();
};

function TcpToAmqpBridge(port, address) {
    log.info('Created TCP to AMQP bridge %s => %s', port, address);
    this.port = port;
    this.address = address;

    this.container = rhea.create_container({enable_sasl_external:true, reconnect:false});
    this.container.id = process.env.HOSTNAME  + '_tcp_' + this.port + '_to_amqp_' + address;
    this.server = net.createServer(this.incoming_connection.bind(this));
    this.server.on('error', function (e) {
        console.error(e);
    });
    var id = this.container.id
    var self = this;
    this.server.listen(this.port, function () {
        log.info('[%s] listening on %s', id, self.server.address().port);
    });
}

function socket_to_string(socket) {
    return client_ip(socket) + ':' + socket.remotePort;
}

TcpToAmqpBridge.prototype.incoming_connection = function (socket) {
    log.info('[%s] socket accepted %s', this.container.id, socket_to_string(socket));
    var client = client_ip(socket);
    var conn_id = metrics.tcp_connection_id(client, socket.remotePort, this.site_id);
    metrics.tcp_ingress_connection_open(this.metrics, conn_id, client);
    var self = this;
    socket.pause();
    var connection = this.container.connect();
    var self = this;
    var client = tunnel.client(connection, this.address, conn_id, function (data) {
        socket.write(data);
        metrics.tcp_ingress_data_out(self.metrics, conn_id, data.length);
    }, function (error) {
        log.info('[%s] tunnel ended %s %s', self.container.id, error, socket_to_string(socket));
        socket.resume();
        socket.end();
    }, function (ready) {
        log.info('[%s] socket %s %s', self.container.id, (ready ? "resumed" : "paused"), socket_to_string(socket));
        if (ready) socket.resume();
        else socket.pause();
    });
    socket.on('data', function (data) {
        client.write({body:data});
        metrics.tcp_ingress_data_in(self.metrics, conn_id, data.length);
    });
    socket.on('end', function () {
        log.info('[%s] socket disconnected %s', self.container.id, socket_to_string(socket));
        metrics.tcp_ingress_connection_close(this.metrics, conn_id);
        client.close();
    });
};

module.exports.amqp_to_http = function (address, host, port) {
    return new AmqpToHttpBridge(address, host, port);
};

module.exports.amqp_to_http2 = function (address, host, port) {
    return myhttp2.egress(address, host, port);
};

module.exports.amqp_to_tcp = function (address, host, port) {
    return new AmqpToTcpBridge(address, host, port);
};

module.exports.http_to_amqp = function (port, address) {
    return new HttpToAmqpBridge(port, address);
};

module.exports.http2_to_amqp = function (port, address) {
    return myhttp2.ingress(port, address);
};

module.exports.tcp_to_amqp = function (port, address) {
    return new TcpToAmqpBridge(port, address);
};
