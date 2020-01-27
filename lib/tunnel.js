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

var amqp = require('rhea');
var log = require('./log.js').logger();
var metrics = require('./metrics.js');

function bind(client, event) {
    client.connection.on(event, client['on_' + event].bind(client));
}

function Client (connection, target, id, data_handler, end_handler, flow_handler) {
    this.requests = [];
    this.connection = connection;
    this.target = target;
    this.id = id;
    this.data_handler = data_handler;
    this.end_handler = end_handler;
    this.flow_handler = flow_handler;
    this.receiver = this.connection.open_receiver({source:{dynamic:true}});
    log.info('client tunnel %s created for %s', this.id, target);
    this.flowing = false;
    bind(this, 'message');
    bind(this, 'receiver_open');
    bind(this, 'receiver_close');
    bind(this, 'sender_close');
    bind(this, 'session_close');
    bind(this, 'sendable');
    bind(this, 'disconnected');
    bind(this, 'released');
    bind(this, 'rejected');
    bind(this, 'modified');
    bind(this, 'accepted');
};

Client.prototype.check_flowing = function () {
    var current = this.ready();
    if (current != this.flowing) {
        this.flowing = current;
        this.flow_handler(this.flowing);
    }
}

Client.prototype.on_message = function (context) {
    this.data_handler(context.message.body);
    log.debug('[%s] received message of %s bytes', this.id, context.message.body.length);
};

Client.prototype.on_receiver_open = function (context) {
    this.address = context.receiver.source.address;
    log.debug('[%s] reply address for client tunnel is %s', this.id, this.address);
    if (this.sender === undefined) {
        this.sender = this.connection.open_sender({name: this.id, target: this.target, source: this.address});
        log.debug('[%s] opened sender back to %s', this.id, this.address);
    }
    this.send_pending_requests();
    this.check_flowing();
};

Client.prototype.on_sendable = function (context) {
    this.send_pending_requests();
    this.check_flowing();
};

Client.prototype.on_released = function (context) {
    this.notify_ended('message released')
};

Client.prototype.on_rejected = function (context) {
    this.notify_ended('message rejected: ' + context.delivery.remote_state.error)
};

Client.prototype.on_modified = function (context) {
    this.notify_ended('message modified')
};

Client.prototype.on_accepted = function (context) {
};

Client.prototype.on_sender_close = function (context) {
    if (context.sender.remote.detach.error) {
        this.notify_ended('sender ' + context.sender.name + ' closed: ' + JSON.stringify(context.sender.remote.detach.error));
    } else {
        this.notify_ended('sender ' + context.sender.name + ' closed');
    }
};

Client.prototype.on_receiver_close = function (context) {
    if (context.receiver.remote.detach.error) {
        this.notify_ended('receiver ' + context.receiver.name + ' closed: ' + JSON.stringify(context.receiver.remote.detach.error));
    } else {
        this.notify_ended('receiver ' + context.receiver.name + ' closed');
    }
};

Client.prototype.on_session_close = function (context) {
    if (context.session.remote.detach.error) {
        this.notify_ended('session closed: ' + JSON.stringify(context.session.remote.detach.error));
    } else {
        this.notify_ended('session closed');
    }
};

Client.prototype.send = function (message) {
    message.reply_to = this.address;
    this.sender.send(message);
};

Client.prototype.send_pending_requests = function () {
    while (this.ready() && this.requests.length > 0) {
        this.send(this.requests.shift());
    }
    return this.requests.length === 0;
};

Client.prototype.send_or_queue = function (message) {
    if (this.ready() && this.send_pending_requests()) {
        this.send(message);
        log.debug('[%s] sending message of %s bytes', this.id, message.body.length);
    } else if (this.sender === undefined || !this.sender.is_closed()) {
        this.requests.push(message);
        log.debug('Have %d requests pending for %s (%s, %s)', this.requests.length, this.id,
                 this.address ? 'address ready': 'address NOT ready', this.sender && this.sender.sendable() ? 'sendable' : 'NOT sendable');
        this.check_flowing();
    } else {
        log.warn('discarding data for %s; sender closed', this.id);
    }
};

Client.prototype.write = function (message) {
    this.send_or_queue(message);
};

Client.prototype.ready = function (details) {
    return this.sender && this.sender.is_open() && this.address !== undefined && this.sender.sendable();
};

Client.prototype.close = function () {
    this.connection.close();
};

Client.prototype.notify_ended = function (error) {
    if (error) log.info('client tunnel %s ended with %s (%d pending)', this.id, error, this.requests.length);
    else log.info('client tunnel %s ended (%d pending)', this.id, this.requests.length);
    if (this.end_handler) {
        this.end_handler(error);
        this.end_handler = undefined;
        process.nextTick(this.close.bind(this));
    }
};

Client.prototype.on_disconnected = function (context) {
    this.address = undefined;
    this.notify_ended('disconnected');
};

function clean_link_name(name) {
    var parts = name.split('@');
    return parts.length >= 2 ? parts[0] + '@' + parts[1] : name;
}

function Server(receiver, socket, host, _metrics) {
    this.receiver = receiver;
    this.socket = socket;
    this.sender = undefined;
    this.conn_id = clean_link_name(receiver.name);

    this.sender = receiver.session.open_sender(receiver.source.address);
    this.sender.on('sender_close', this.on_sender_close.bind(this));
    log.info('server tunnel created for socket %s, %s <-> %s', this.conn_id, receiver.source.address, receiver.target.address);
    this.socket_closed = false;
    this.links_closed = false;
    this.receiver.on('message', this.on_message.bind(this));
    this.receiver.on('receiver_close', this.on_receiver_close.bind(this));
    this.session_close_handler = this.on_session_close.bind(this)
    this.receiver.session.on('session_close', this.session_close_handler);
    this.socket.on('data', this.on_socket_data.bind(this));
    this.socket.on('end', this.on_socket_end.bind(this));
    this.socket.on('error', this.on_socket_error.bind(this));
    this.metrics = _metrics;
    metrics.tcp_egress_connection_open(this.metrics, this.conn_id, host);
}

Server.prototype.on_message = function (context) {
    if (!this.socket_closed) {
        this.socket.write(context.message.body);
        log.debug('%s relayed message of %s bytes', this.conn_id, context.message.body.length);
        metrics.tcp_egress_data_out(this.metrics, this.conn_id, context.message.body.length);
    } else {
        log.warn('%s discarding message as socket is closed', this.conn_id);
    }
};

Server.prototype.on_receiver_close = function (context) {
    log.info('receiver %s closed for socket %s', context.receiver.name, this.conn_id)
    this.close_socket();
};

Server.prototype.on_sender_close = function (context) {
    log.info('sender %s closed for socket %s', context.sender.name, this.conn_id)
    this.close_socket();
};

Server.prototype.on_session_close = function (context) {
    log.info('session closed for socket %s', this.conn_id)
    this.close_socket();
};

Server.prototype.close_socket = function () {
    if (!this.socket_closed) {
        this.socket_closed = true;
        this.socket.end();
    }
};

Server.prototype.on_socket_data = function (data) {
    this.sender.send({body:data});
    metrics.tcp_egress_data_in(this.metrics, this.conn_id, data.length);
    log.info('%s got data from socket of %s bytes', this.conn_id, data.length);
};

Server.prototype.on_socket_end = function (data) {
    if (data) {
        this.on_socket_data(data);
    }
    log.error('socket ended %s', this.conn_id);
    metrics.tcp_egress_connection_close(this.metrics, this.conn_id);
    this.socket_closed = true;
    process.nextTick(this.close.bind(this));
};

Server.prototype.on_socket_error = function (error) {
    this.socket_closed = true;
    log.error('on_socket_error %s: %s', this.conn_id, error);
    process.nextTick(this.close.bind(this));
};

Server.prototype.close = function () {
    if (!this.links_closed) {
        this.receiver.session.removeListener('session_close', this.session_close_handler);
        this.receiver.close();
        if (this.sender) {
            this.sender.close();
        }
        log.info('server tunnel closed links for socket %s', this.conn_id);
    }
}

module.exports.client = function (connection, target, id, data_handler, end_handler, flow_handler) {
    return new Client(connection, target, id, data_handler, end_handler, flow_handler);
};

module.exports.server = function (receiver, socket, host, _metrics) {
    return new Server(receiver, socket, host, _metrics);
};
