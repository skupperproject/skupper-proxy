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

function bind(client, event) {
    client.connection.on(event, client['on_' + event].bind(client));
}

function Client (connection, target) {
    this.requests = [];
    this.incomplete = {};
    this.handlers = {};
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

Client.prototype.on_message = function (context) {
    var handler = this.handlers[context.message.correlation_id];
    if (handler) {
        delete this.handlers[context.message.correlation_id];
        handler.resolve(context.message);
    }
};

Client.prototype.on_receiver_open = function (context) {
    this.address = context.receiver.source.address;
    this.send_pending_requests();
};

Client.prototype.on_sendable = function (context) {
    this.send_pending_requests();
};

Client.prototype.on_released = function (context) {
    var msg = this.incomplete[context.delivery.id];
    if (msg) {
        delete this.incomplete[context.delivery.id];
        var handler = this.handlers[msg.correlation_id];
        if (handler) {
            if (handler.released === 0) {
                handler.released++;
                this.send_or_queue(msg);
            } else if (handler.released < this.max_resends) {
                var self = this;
                setTimeout(function () {
                    self.send_or_queue(msg);
                }, Math.min(50 * handler.released, this.max_resend_delay));
                handler.released++;
            } else {
                this.failed(msg.correlation_id, 'Could not deliver request');
            }
        }
    }
};

Client.prototype.on_rejected = function (context) {
    this.delivery_failure(context.delivery.id, context.delivery.remote_state.error);
};

Client.prototype.on_modified = function (context) {
    this.delivery_failure(context.delivery.id, 'Request failed');
};

Client.prototype.on_accepted = function (context) {
    delete this.incomplete[context.delivery.id];
};

Client.prototype.delivery_failure = function (id, error) {
    var msg = this.incomplete[id];
    if (msg) {
        delete this.incomplete[id];
        this.failed(msg.correlation_id, error);
    } else {
        log.error('Cannot find correlation id for failed delivery');
    }
};

Client.prototype.failed = function (correlation_id, error) {
    var handler = this.handlers[correlation_id];
    if (handler) {
        delete this.handlers[correlation_id];
        handler.reject(error);
    } else {
        log.error('Cannot find handler for failed delivery');
    }
};

Client.prototype.send = function (message) {
    message.reply_to = this.address;
    var delivery = this.sender.send(message);
    this.incomplete[delivery.id] = message;
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

Client.prototype.request = function (message) {
    message.correlation_id = this.counter++;
    var handlers = this.handlers;
    var promise =  new Promise(function (resolve, reject) {
        handlers[message.correlation_id] = {resolve:resolve, reject:reject, released:0};
    });
    this.send_or_queue(message);
    return promise;
};

Client.prototype.ready = function (details) {
    return this.address !== undefined && this.sender.sendable();
};

Client.prototype.close = function () {
    this.connection.close();
};

Client.prototype.on_disconnected = function (context) {
    this.address = undefined;
    for (var key in this.handlers) {
        this.handlers[key].reject('aborted');
    }
    this.handlers = {};
};

module.exports.create = function (connection, target) {
    return new Client(connection, target);
};
