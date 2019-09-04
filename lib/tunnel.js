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

function bind(client, event) {
    client.connection.on(event, client['on_' + event].bind(client));
}

function Client (connection, target, data_handler, end_handler) {
    this.requests = [];
    this.connection = connection;
    this.data_handler = data_handler;
    this.end_handler = end_handler;
    this.sender = this.connection.open_sender(target);
    this.receiver = this.connection.open_receiver({source:{dynamic:true}});
    this.counter = 1;
    this.max_resends = 10;
    this.max_resend_delay = 250;
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

Client.prototype.on_message = function (context) {
    this.data_handler(context.message.body);
};

Client.prototype.on_receiver_open = function (context) {
    this.address = context.receiver.source.address;
    this.send_pending_requests();
};

Client.prototype.on_sendable = function (context) {
    this.send_pending_requests();
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
        this.notify_ended('sender closed: ' + JSON.stringify(context.sender.remote.detach.error));
    } else {
        this.notify_ended('sender closed');
    }
};

Client.prototype.on_receiver_close = function (context) {
    if (context.receiver.remote.detach.error) {
        this.notify_ended('receiver closed: ' + JSON.stringify(context.receiver.remote.detach.error));
    } else {
        this.notify_ended('receiver closed');
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

Client.prototype.send_pending_requests = function (context) {
    while (this.ready() && this.requests.length > 0) {
        this.send(this.requests.shift());
    }
    return this.requests.length === 0;
};

Client.prototype.send_or_queue = function (message) {
    if (this.ready() && this.send_pending_requests()) {
        this.send(message);
    } else {
        this.requests.push(message);
    }
};

Client.prototype.write = function (message) {
    this.send_or_queue(message);
};

Client.prototype.ready = function (details) {
    return this.address !== undefined && this.sender.sendable();
};

Client.prototype.close = function () {
    this.connection.close();
};

Client.prototype.notify_ended = function (error) {
    if (this.end_handler) {
        this.end_handler(error);
        this.end_handler = undefined;
    }
};

Client.prototype.on_disconnected = function (context) {
    this.address = undefined;
    this.notify_ended('disconnected');
};

function Server(receiver, socket) {
    this.receiver = receiver;
    this.socket = socket;
    this.sender = undefined;
    this.receiver.on('message', this.on_message.bind(this));
    this.receiver.on('receiver_close', this.on_receiver_close.bind(this));
    this.receiver.on('sender_close', this.on_sender_close.bind(this));
    this.receiver.on('session_close', this.on_session_close.bind(this));
    this.socket.on('data', this.on_socket_data.bind(this));
    this.socket.on('end', this.on_socket_end.bind(this));
    this.socket.on('error', this.on_socket_error.bind(this));
}

Server.prototype.on_message = function (context) {
    if (this.sender === undefined) {
        this.sender = context.connection.open_sender(context.message.reply_to);
    }
    this.socket.write(context.message.body);
};

Server.prototype.on_receiver_close = function (context) {
    this.socket.end();
};

Server.prototype.on_sender_close = function (context) {
    this.socket.end();
};

Server.prototype.on_session_close = function (context) {
    this.socket.end();
};

Server.prototype.on_socket_data = function (data) {
    this.sender.send({body:data});
};

Server.prototype.on_socket_end = function (data) {
    if (data) {
        this.on_socket_data(data);
    }
    process.nextTick(this.close.bind(this));
};

Server.prototype.on_socket_error = function (error) {
    console.error(error);
    process.nextTick(this.close.bind(this));
};

Server.prototype.close = function () {
    this.receiver.close();
    if (this.sender) {
        this.sender.close();
    }
}

module.exports.client = function (connection, target, data_handler, end_handler) {
    return new Client(connection, target, data_handler, end_handler);
};

module.exports.server = function (receiver, socket) {
    return new Server(receiver, socket);
};
