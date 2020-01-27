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

var EventEmitter = require('events');
var http = require('http');
var util = require('util');
var rhea = require('rhea');
var url_parse = require('url').parse;

var log = require('./log.js').logger();
var metrics = require('./metrics.js');
var rclient = require('./request.js');
var client_ip = require('./utils.js').client_ip;

function elapsed(start) {
    return Date.now() - start;
}

function AmqpToHttpBridge(address, host, port) {
    log.info('Created AMQP to HTTP bridge %s => %s:%s', address, host, port);
    this.host = host;
    this.port = port;
    this.address = address;
    this.delivery_mapping = {};
    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_amqp:' + this.address + '=>http://' + this.host  + ':' + this.port;
    this.connection = container.connect();
    this.connection.on('message', this.incoming.bind(this));
    this.connection.on('settled', this.on_response_settled.bind(this));
    this.connection.open_receiver({source:address, autoaccept:false,credit_window:10/*TODO: make configurable*/});
}

AmqpToHttpBridge.prototype.incoming = function (context) {
    var start = Date.now();
    var url = url_parse('http://' + context.message.to);
    var headers = {};
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
        log.info('%s: %s', url, response.statusCode);
        var message_out = {
            to: context.message.reply_to,
            correlation_id: context.message.correlation_id,
            subject: '' + response.statusCode,
            application_properties: {},
            body: ''
        };
        for (var key in response.headers) {
            if (key === 'content-type') {
                message_out.content_type = response.headers['content-type'];
            } else {
                message_out.application_properties[key] = response.headers[key]
            }
        }
	response.on('data', function (chunk) { message_out.body += chunk; });
	response.on('end', function () {
            log.info('server sending reply: %j', message_out);
            var response_delivery = context.connection.send(message_out);
            self.delivery_mapping[response_delivery.id] = context.delivery;
            metrics.http_egress_request(self.metrics, self.host, options.method, response.statusCode, context.message.body.length, message_out.body.length, elapsed(start));
        });
    });
    request.on('error', function (error) {
        console.error(error);
        context.delivery.modified();
    });
    request.write(context.message.body);
    request.end();
};

AmqpToHttpBridge.prototype.on_response_settled = function (context) {
    var response_delivery = context.delivery;
    var request_delivery =  this.delivery_mapping[response_delivery.id];
    request_delivery.accept();
}

AmqpToHttpBridge.prototype.stop = function () {
    this.connection.close();
};

function Request (client, id, request) {
    EventEmitter.call(this);
    this.client = client;
    this.id = id;
    this.request = request;
    this.request.correlation_id = this.id;
    this.resends = 0;
}

util.inherits(Request, EventEmitter);

Request.prototype.handle_response = function (response) {
    this.emit('data', response);
    log.info('Request %s got response %j', this.id, response);
};

Request.prototype.send = function () {
    this.request.reply_to = this.client.address;
    this.client.sender.send(this.request, this.id);
};

Request.prototype.released = function () {
    log.info('Request %s released', this.id);
    if (this.resends === 0) {
        this.resends++;
        this.send(sender);
    } else if (this.resends < this.client.max_resends) {
        var self = this;
        setTimeout(function () {
            self.send();
        }, Math.min(50 * this.resends, this.client.max_resend_delay));
        this.resends++;
    } else {
        this.failed('Could not deliver request, max resends exceeded');
    }
};

Request.prototype.failed = function (error) {
    log.info('Request %s failed: %s', this.id, error);
    this.emit('error', error);
    this.completed();
};

Request.prototype.completed = function () {
    log.info('Request %s completed', this.id);
    this.emit('end');
    this.client.discard(this.id);
};

Request.prototype.as_promise = function () {
    var self = this;
    var responses = [];
    var error = undefined;
    return new Promise(function (resolve, reject) {
        self.on('data', function (response) {
            responses.push(response);
        });
        self.on('error', function (e) {
            error = e;
        });
        self.on('end', function () {
            if (error) {
                reject(error);
            } else if (responses.length === 1) {
                resolve(responses[0]);
            } else if (responses.length > 1) {
                resolve(responses);
            }
        });
    });
};

function bind(client, event) {
    client.connection.on(event, client['on_' + event].bind(client));
}

function RequestClient (connection, target) {
    this.requests = {};
    this.pending = [];
    this.connection = connection;
    this.sender = this.connection.open_sender(target);
    this.receiver = this.connection.open_receiver({source:{dynamic:true}});
    this.counter = 1;
    this.max_resends = 10;
    this.max_resend_delay = 250;
    bind(this, 'message');
    bind(this, 'receiver_open');
    bind(this, 'sendable');
    bind(this, 'disconnected');
    bind(this, 'released');
    bind(this, 'rejected');
    bind(this, 'modified');
    bind(this, 'accepted');
};

RequestClient.prototype.discard = function (request_id) {
    delete this.requests[request_id];
};

RequestClient.prototype.on_message = function (context) {
    var handler = this.requests[context.message.correlation_id];
    if (handler) {
        handler.handle_response(context.message);
    } else {
        log.info('Unrecognised response: %s', context.message.correlation_id);
    }
};

RequestClient.prototype.on_receiver_open = function (context) {
    this.address = context.receiver.source.address;
    this.send_pending_requests();
};

RequestClient.prototype.on_sendable = function (context) {
    this.send_pending_requests();
};

RequestClient.prototype.on_released = function (context) {
    var request = this.requests[context.delivery.tag];
    if (request) {
        request.released();
    }
};

RequestClient.prototype.on_rejected = function (context) {
    this.delivery_failure(context.delivery.tag, context.delivery.remote_state.error);
};

RequestClient.prototype.on_modified = function (context) {
    //TODO: retry?
    this.delivery_failure(context.delivery.tag, 'Request failed');
};

RequestClient.prototype.on_accepted = function (context) {
    var request = this.requests[context.delivery.tag];
    if (request) {
        request.completed();
    }
};

RequestClient.prototype.delivery_failure = function (id, error) {
    var request = this.requests[id];
    if (request) {
        request.failed(error);
    }
};

RequestClient.prototype.send_pending_requests = function (context) {
    while (this.ready() && this.pending.length > 0) {
        this.pending.shift().send();
    }
    return this.pending.length === 0;
};

RequestClient.prototype.send_or_queue = function (request) {
    if (this.ready() && this.send_pending_requests()) {
        request.send();
    } else {
        this.pending.push(request);
    }
};

RequestClient.prototype.next_id = function () {
    return 'req-' + this.counter++;
};

RequestClient.prototype.request = function (message) {
    return this.submit(message).as_promise();
};

RequestClient.prototype.submit = function (message) {
    var id = this.next_id();
    var request = new Request(this, id, message);
    this.requests[id] = request;
    this.send_or_queue(request);
    return request;
};

RequestClient.prototype.ready = function (details) {
    return this.address !== undefined && this.sender.sendable();
};

RequestClient.prototype.close = function () {
    this.connection.close();
};

RequestClient.prototype.on_disconnected = function (context) {
    this.address = undefined;
    for (var id in this.requests) {
        //TODO: could try and resend on reconnecting...
        this.delivery_failure(id, 'aborted');
    }
};

function HttpToAmqpBridge(port, address, aggregator) {
    log.info('Created HTTP to AMQP bridge %s => %s', port, address);
    this.port = port;
    this.address = address;
    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_http:' + this.port + '=>amqp:' + address;
    this.client = new RequestClient(container.connect(), address);
    this.aggregator = aggregator;

    //listen for http requests
    this.server = http.createServer(this.request.bind(this));
    this.server.listen(this.port, '0.0.0.0');
    log.info('listening for http on %s', this.port);
}

const skipped_headers = ['CONNECTION', 'KEEP-ALIVE'];

function write_header(response, key, value) {
    response.write(key + ': ' + value + '\r\n');
}

const boundary_marker = 'skupper-boundary';
function write_multipart_aggregated_response(response, messages_in) {
    var response_bytes = 0;
    for (var i = 0; i < messages_in.length; i++) {
        var message_in = messages_in[i];
        if (i == 0) {
            response.writeHead(200/* Is this right? */, {'content-type': 'multipart/mixed; boundary='+ boundary_marker});
        }
        log.info('got reply for outbound request: %s %s', message_in.subject, message_in.to);
        if (message_in.content_type) {
            write_header(response, 'content-type', message_in.content_type);
        }
        for (var key in message_in.application_properties) {
            if (skipped_headers.indexOf(key.toUpperCase()) < 0) {
                write_header(response, key, message_in.application_properties[key]);
            }
        }
        response.write('\r\n');
        response.write(message_in.body);
        if (i == (messages_in.length - 1)) response.write('\r\n--' + boundary_marker + '--\r\n');
        else response.write('\r\n--' + boundary_marker + '\r\n');
        response_bytes += message_in.body.length;
    }
    response.end();
    return response_bytes;
}

function write_json_aggregated_response(response, messages_in) {
    var aggregated = [];
    var headers = {};
    for (var i = 0; i < messages_in.length; i++) {
        var message_in = messages_in[i];
        var part = {};
        if (message_in.content_type === 'application/json') {
            try {
                part.content = JSON.parse(message_in.body);
            } catch (e) {
                part.error = e;
            }
        } else {
            part.error = 'Wrong content-type. Expected application/json, got ' + message_in.content_type;
        }
        aggregated.push(part);
        for (var key in message_in.application_properties) {
            if (skipped_headers.indexOf(key.toUpperCase()) < 0 && key !== 'content-type' && key !== 'content-length') {
                headers[key] = message_in.application_properties[key];
            }
        }
    }
    response.statusCode = 200;
    response.setHeader('content-type', 'application/json');
    for (var key in headers) {
        response.setHeader(key, headers[key]);
    }
    var content = JSON.stringify(aggregated);
    response.end(content);
    return content.length;
}

HttpToAmqpBridge.prototype.request = function (request, response) {
    var start = Date.now();
    var self = this;
    var url = url_parse(request.url);
    var path = url.pathname;
    var address = request.headers.host ? request.headers.host.split(':')[0] + path : url_parse(request.url);
    log.info('outgoing request %s (%s)', request.headers.host, address);

    var body = '';
    request.on('data', function (data) { body += data; });
    request.on('end', function () {
        var message_out = {
            to: address,
            subject: request.method,
            application_properties: {},
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
        self.client.request(message_out).then(function (messages_in) {
            if (messages_in.length === 0) {
                response.writeHead(204/** Is this right**/, {});
                response.end();
            } else {
                var response_bytes = self.aggregator(response, messages_in);
                metrics.http_ingress_request(self.metrics, client_ip(request.socket), request.method, response.statusCode, message_out.body.length, response_bytes, elapsed(start));
            }
        }).catch(function (error) {
            console.error(error);
            response.statusCode = 500;
            response.end(error.toString());
            metrics.http_ingress_request(self.metrics, client_ip(request.socket), request.method, response.statusCode, message_out.body.length, error.toString().length, elapsed(start));
        });
    });
};

module.exports.amqp_to_http = function (address, host, port) {
    return new AmqpToHttpBridge('mc/'+address, host, port);
};

module.exports.http_to_amqp = function (port, address, aggregator) {
    if (aggregator === 'json') {
        return new HttpToAmqpBridge(port, 'mc/'+address, write_json_aggregated_response);
    } else { //aggregator === 'multipart'
        return new HttpToAmqpBridge(port, 'mc/'+address, write_multipart_aggregated_response);
    }
};
