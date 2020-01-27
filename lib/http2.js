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

var http2 = require('http2');
var rhea = require('rhea');

var log = require('./log.js').logger();
var metrics = require('./metrics.js');
var client_ip = require('./utils.js').client_ip;

function elapsed(start) {
    return Date.now() - start;
}

function is_end_stream(message) {
    return message.application_properties && message.application_properties.ended;
}

function Ingress (port, address) {
    this.port = port;
    this.address = address;
    this.server = http2.createServer();
    this.server.on('stream', this.on_stream.bind(this));
    this.server.on('error', (err) => console.error(err));
    this.server.listen(this.port);

    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_http:' + this.port + '=>amqp:' + this.address;
    this.connection = container.connect();
    this.sender = this.connection.open_sender(this.address);
    this.sender.on('sendable', this.on_sendable.bind(this));
    var recv = this.connection.open_receiver({source:{dynamic:true}});
    recv.on('receiver_open', this.on_receiver_open.bind(this));
    recv.on('message', this.on_message.bind(this));

    this.reply_to = undefined;
    this.streams = {};
    this.requests = [];
    this.incomplete = {};
    this.max_resends = 10;
    this.max_resend_delay = 250;
    log.info('Created bridge for http2:%s->amqp:%s', this.port, this.address)
}

Ingress.prototype.on_stream = function (stream, headers, flags) {
    log.debug('ingress started stream %s %j FLAGS=%s', stream.id, headers, flags.toString(2));
    this.streams[stream.id] = new IngressStream(this, stream, headers);
};

Ingress.prototype.on_receiver_open = function (context) {
    this.reply_to = context.receiver.source.address;
    this.send_pending_requests();
};

Ingress.prototype.on_sendable = function (context) {
    this.send_pending_requests();
};

Ingress.prototype.on_released = function (context) {
    var msg = this.incomplete[context.delivery.id];
    if (msg) {
        delete this.incomplete[context.delivery.id];
        var stream = this.streams[msg.correlation_id];
        if (stream) {
            if (stream.released === 0) {
                stream.released++;
                this.send_or_queue(msg);
            } else if (stream.released < this.max_resends) {
                var self = this;
                setTimeout(function () {
                    self.send_or_queue(msg);
                }, Math.min(50 * handler.released, this.max_resend_delay));
                stream.released++;
            } else {
                this.failed(msg.correlation_id, 'Could not deliver request');
            }
        }
    }
};

Ingress.prototype.on_rejected = function (context) {
    this.delivery_failure(context.delivery.id, context.delivery.remote_state.error);
};

Ingress.prototype.on_modified = function (context) {
    this.delivery_failure(context.delivery.id, 'Request failed');
};

Ingress.prototype.on_accepted = function (context) {
    delete this.incomplete[context.delivery.id];
};

Ingress.prototype.delivery_failure = function (id, error) {
    var msg = this.incomplete[id];
    if (msg) {
        delete this.incomplete[id];
        this.failed(msg.correlation_id, error);
    }
};

Ingress.prototype.failed = function (correlation_id, error) {
    var stream = this.streams[correlation_id];
    if (stream) {
        stream.failed(error);
        delete this.streams[correlation_id];
    }
};

Ingress.prototype.ready = function (details) {
    return this.reply_to !== undefined && this.sender.sendable();
};

Ingress.prototype.on_message = function (context) {
    var stream = this.streams[context.message.correlation_id];
    if (stream) {
        stream.on_message(context);
        if (is_end_stream(context.message)) {
            delete this.streams[context.message.correlation_id];
        }
    } else if (context.message.correlation_id) {
        log.error('Stream not active: %s', context.message.correlation_id);
    } else {
        log.error('Response has no correlation id: %j', context.message);
    }
};

Ingress.prototype.request = function (message) {
    this.send_or_queue(message);
};

Ingress.prototype.send = function (message) {
    message.reply_to = this.reply_to;
    var delivery = this.sender.send(message);
    this.incomplete[delivery.id] = message;
    log.debug('Ingress sent request %j', message);
};

Ingress.prototype.send_pending_requests = function (context) {
    while (this.ready() && this.requests.length > 0) {
        this.send(this.requests.shift());
    }
    return this.requests.length === 0;
};

Ingress.prototype.send_or_queue = function (message) {
    if (this.ready() && this.send_pending_requests()) {
        this.send(message);
    } else {
        this.requests.push(message);
    }
};

function IngressStream (ingress, stream, headers) {
    this.ingress = ingress;
    this.stream = stream;
    this.headers = headers;
    this.first_request_sent = false;
    this.first_response_received = false;
    this.request_ended = false;
    this.response_initiated = false;
    this.incoming = [];
    this.start = Date.now();
    this.bytes_in = 0;
    this.bytes_out = 0;
    this.stream.on('data', this.on_data.bind(this));
    this.stream.on('end', this.on_end.bind(this));
    this.stream.on('wantTrailers', this.on_want_trailers.bind(this));
}

IngressStream.prototype.failed = function (error) {
    log.error('Ingress failed: %s', error);
    this.stream.respond({ ':status': 500 }, { endStream: true });
};

function headers_to_message(headers, message) {
    for (var k in headers) {
        if (k === ':path') message.to = headers[k];
        else if (k === ':method') message.subject = headers[k];
        else if (k === 'content-type') message.content_type = headers[k];
        else message.application_properties[k] = headers[k];
    }
}

function headers_from_message(message) {
    var headers = {};
    for (var k in message.application_properties) {
        if (k !== 'ended') {
            headers[k] =  message.application_properties[k];
        }
    }
    if (message.subject && message.subject !== 'response') {
        headers[':method'] = message.subject;
    }
    headers[':path'] = message.to;
    headers['content-type'] = message.content_type;
    return headers;
}

IngressStream.prototype.on_data = function (data) {
    log.debug('Ingress got data (%s bytes)', data ? data.length : 0);
    if (data) this.bytes_in += data.length;
    this.incoming.push(data);
    this.flush_timer = setTimeout(this.do_send.bind(this), 1);
};

IngressStream.prototype.on_end = function (data) {
    log.debug('ingress stream ended');
    this.request_ended = true;
    if (data) this.incoming.push(data);
    if (this.flush_timer) clearTimeout(this.flush_timer);
    this.do_send();
    if (this.sender) {
        this.sender.on('settled', function (context) {
            context.sender.close();
        });
    }
};

IngressStream.prototype.do_send = function () {
    var data = this.incoming[0]
    var msg = {correlation_id: this.stream.id, group_id: this.ingress.site_id, application_properties: {'ended': this.request_ended}, body: data};
    if (!this.first_request_sent) {
        log.info('Ingress sending request for %s (%s bytes)', this.stream.id, data ? data.length : 0);
        this.first_request_sent = true;
        headers_to_message(this.headers, msg);
        this.ingress.request(msg);
        this.incoming.shift();
        if (!this.request_ended) this.stream.pause();
    } else if (this.first_response_received) {
        log.info('Ingress sent stream data for %s (%s bytes)', this.stream.id, data ? data.length : 0);
        this.sender.send(msg);
        this.incoming.shift();
    } else {
        log.info('Ingress cannot stream data for %s, first response not yet received', this.stream.id);
    }
};

IngressStream.prototype.on_want_trailers = function () {
    if (this.trailers) {
        log.debug('ingress stream %s sending trailers %j', this.stream.id, this.trailers);
        this.stream.sendTrailers(this.trailers);
    } else {
        this.stream.close();
    }
};

IngressStream.prototype.on_message = function (context) {
    if (!this.first_response_received) {
        this.first_response_received = true;
        this.stream.resume();
        if (context.message.reply_to === undefined && !this.request_ended) {
            //error
            log.error('response for stream does not have reply address');
            this.stream.respond({ ':status': 500 });
        } else {
            if (!this.request_ended || this.incoming.length > 0) {
                this.sender = this.ingress.connection.open_sender(context.message.reply_to);
                while (this.incoming.length > 0) {
                    this.do_send();
                }
            }
        }
    }
    if (!this.response_initiated && context.message.subject === 'response') {
        var headers = headers_from_message(context.message);
        this.status = headers[':status'];
        log.debug('ingress sent response for %s: %j', this.stream.id, headers);
        this.stream.respond(headers, { waitForTrailers:true });
        this.response_initiated = true;
    }
    if (context.message.footer) {
        this.trailers = context.message.footer;
    }
    if (context.message.body) {
        this.bytes_out += context.message.body.length;
    }
    if (context.message.application_properties.ended) {
        if (context.message.body) {
            log.debug('ingress ending stream (%s bytes)', context.message.body.length );
            this.stream.end(context.message.body);
        } else {
            log.debug('ingress ending stream (0 bytes)');
            this.stream.end();
        }
        metrics.http_ingress_request(this.ingress.metrics, client_ip(this.stream.session.socket), this.headers[':method'], this.status, context.message.group_id, this.bytes_in, this.bytes_out, elapsed(this.start));
    } else if (context.message.body) {
        log.info('ingress wrote to stream (%s bytes)', context.message.body.length);
        this.stream.write(context.message.body);
    }
};

function Egress (host, port, address) {
    this.host = host;
    this.port = port;
    this.address = address;

    this.http2_connect();

    var container = rhea.create_container({enable_sasl_external:true});
    container.id = process.env.HOSTNAME + '_amqp:' + this.address + '=>http2://' + this.host  + ':' + this.port;
    this.connection = container.connect();
    this.connection.open_receiver(this.address).on('message', this.on_request_message.bind(this));
    var recv = this.connection.open_receiver({source:{dynamic:true}});
    recv.on('receiver_open', this.on_dynamic_receiver_open.bind(this));
    recv.on('message', this.on_stream_message.bind(this));

    this.reply_to = undefined;
    this.streams = {};
}

Egress.prototype.http2_connect = function () {
    this.client = http2.connect('http://' + this.host + ':' + this.port);
    this.client.on('close', this.on_client_closed.bind(this));
    this.client.on('error', this.on_client_error.bind(this));
};

Egress.prototype.on_client_closed = function () {
    log.info('http2 connection closed; reconnecting');
    this.http2_connect();
};

Egress.prototype.on_client_error = function (error) {
    log.info('http2 connection error: %s', error);
};

Egress.prototype.on_dynamic_receiver_open = function (context) {
    this.reply_to = context.receiver.source.address;
};

Egress.prototype.on_request_message = function (context) {
    log.debug('Egress got request: %j', context.message);
    var headers = headers_from_message(context.message);
    var ended = is_end_stream(context.message);
    log.debug('Making request with %j %s', headers, ended ? 'ended' : 'not ended');
    if (ended) {
        var request;
        if (context.message.body) {
            log.debug('...ended with %s bytes of data', context.message.body.length);
            request = this.client.request(headers, {endStream:false})
            request.end(context.message.body);
        } else {
            log.debug('...ended without data');
            request = this.client.request(headers, {endStream:true});
        }
        //create egress stream instance to handle response; won't get any further messages for it so don't need to
        //put it in this.streams
        var egress_stream = new EgressStream(this.connection.open_sender(context.message.reply_to), request, this.reply_to, context.message.correlation_id, context.message.group_id, this);
        if (context.message.body) {
            egress_stream.bytes_in += context.message.body.length;
        }
    } else {
        var request = this.client.request(headers, {endStream:false});
        request.write(context.message.body);
        log.debug('...wrote %s bytes of data', context.message.body.length);
        var egress_stream = new EgressStream(this.connection.open_sender(context.message.reply_to), request, this.reply_to, context.message.correlation_id, context.message.group_id, this);
        if (context.message.body) {
            egress_stream.bytes_in += context.message.body.length;
        }
        this.streams[context.message.correlation_id] = egress_stream;
        egress_stream.ensure_response();
    }
};

Egress.prototype.on_stream_message = function (context) {
    log.info('Egress got stream data');
    var stream = this.streams[context.message.correlation_id];
    if (stream) {
        stream.on_message(context);
        if (is_end_stream(context.message)) {
            delete this.streams[context.message.correlation_id];
        }
    } else if (context.message.correlation_id) {
        log.error('Got message for unknown stream: %s', context.message.correlation_id);
    } else {
        log.error('Got message with no correlation id: %j', context.message);
    }
};

function EgressStream(sender, request, reply_to, correlation_id, client_site, parent) {
    this.request = request;
    this.sender = sender;
    this.reply_to = reply_to;
    this.correlation_id = correlation_id;
    this.parent = parent;
    this.host = parent.host;
    this.site_id = parent.site_id;
    this.client_site = client_site;
    this.headers = undefined;
    this.trailers = undefined;
    this.responses = [];
    this.request_ended = false;
    this.responded = false;
    this.start = Date.now();
    this.bytes_in = 0;
    this.bytes_out = 0;
    this.request.on('response', this.on_response.bind(this));
    this.request.on('trailers', this.on_trailers.bind(this));
    this.request.on('data', this.on_data.bind(this));
    this.request.on('end', this.on_end.bind(this));
}

EgressStream.prototype.on_message = function (context) {
    log.info('egress received stream data for %s', this.request.id);
    if (context.message.body) {
        this.bytes_in += context.message.length;
    }
    if (context.message.application_properties.ended) {
        this.request.end(context.message.body);
    } else {
        this.request.write(context.message.body);
    }
};

EgressStream.prototype.on_data = function (data) {
    log.debug('egress on_data');
    this.bytes_out += data.length;
    this.responses.push(data);
    this.ensure_response();
};

EgressStream.prototype.ensure_response = function () {
    this.timed_flush = setTimeout(this.flush_responses.bind(this),1);
};

EgressStream.prototype.on_end = function (data) {
    log.debug('egress on_end');
    this.request_ended = true;
    if (data) {
        this.bytes_out += data.length;
        this.responses.push(data);
    }
    metrics.http_egress_request(this.parent.metrics, this.host, this.request.sentHeaders[':method'], this.status, this.client_site, this.bytes_in, this.bytes_out, elapsed(this.start));
    if (this.timed_flush) clearInterval(this.timed_flush);
    this.flush_responses(this);
};

EgressStream.prototype.on_response = function (headers, flags) {
    this.headers = headers;
    this.status = this.headers[':status'];
    if (this.request.endAfterHeaders) {
        this.request_ended = true;
        process.nextTick(this.flush_responses.bind(this));
    }
};

EgressStream.prototype.on_trailers = function (headers, flags) {
    log.debug('Got trailers for stream %s: %j %s', this.request.id, headers, flags);
    this.trailers = headers;
};

EgressStream.prototype.flush_responses = function () {
    while (this.responses.length > 1) {
        this.send_response(this.responses.shift(), false);
        this.responded = true;
    }
    if (this.request_ended || this.responses.length > 0 || !this.responded) {
        var data;
        if (this.responses.length) {
            data = this.responses.shift();
        }
        this.send_response(data, this.request_ended);
        this.responded = true;
    }
};

EgressStream.prototype.send_response = function (data, ended) {
    log.info('egress sending response data for %s (%s)', this.request.id, ended);
    var response = {correlation_id: this.correlation_id, group_id: this.site_id, reply_to:this.reply_to, application_properties:{ended:ended}, body:data};
    if (this.headers) {
        headers_to_message(this.headers, response);
        this.headers = undefined;
        response.subject = 'response';
    }
    if (ended && this.trailers) {
        response.footer = this.trailers;
    }
    log.debug('egress sending %j', response);
    this.sender.send(response);
    if (ended && this.responses.length === 0) {
        this.sender.on('settled', function (context) {
            context.sender.close();
        });
    }
};

module.exports.ingress = function (port, address) {
    return new Ingress(port, address);
};

module.exports.egress = function (address, host, port) {
    return new Egress(host, port, address);
};
