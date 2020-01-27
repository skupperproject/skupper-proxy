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
var kubernetes = require('./kubernetes.js').client();
var labels = require('./labels.js');
var log = require('./log.js').logger();
var myutils = require('./utils.js');
var owner_ref = require("../lib/owner_ref.js");
var service_utils = require('./service_utils.js');

const SERVICE_SYNC_ADDRESS="mc/$skupper-service-sync"

function is_success_code(code) {
    return code >= 200 && code < 300;
}

function equivalent_ports(a, b) {
    if (!a) return !b;
    if (a.length != b.length) return false;
    for (var i = 0 ; i < a.length; i++) {
        if (a[i].name !== b[i].name || a[i].port !== b[i].port) {
            return false;
        }
    }
    return true;
}

function equivalent_target (a, b) {
    return a.name === b.name && a.weight === b.weight && a.targetPort === b.targetPort && a.selector === b.selector && a.filter === b.filter;
}

function headless_match(a, b) {
    if (a === undefined) return b === undefined;
    if (b === undefined) return a === undefined;
    return a.name === b.name && a.size === b.size && a.targetPort === b.targetPort;
}

function equivalent_targets (a, b) {
    if (a === undefined) return b === undefined;
    if (b === undefined) return a === undefined;
    if (a.length !== b.length) return false;

    a.sort(name_compare);
    b.sort(name_compare);
    return myutils.changes(a, b, name_compare, equivalent_target) === undefined;
}

function equivalent_service_record (a, b) {
    return a.address === b.address && a.protocol === b.protocol && a.port === b.port && headless_match(a.headless, b.headless) && equivalent_targets(a.targets, b.targets);
}

function name_compare (a, b) {
    return myutils.string_compare(a.name, b.name);
}

function address_compare (a, b) {
    return myutils.string_compare(a.address, b.address);
}

function bind(client, event) {
    client.connection.on(event, client['on_' + event].bind(client));
}

function ServiceSync (origin) {
    this.origin = origin
    this.local = undefined;//track local origin skuppered services
    this.container = rhea.create_container({id:'service-sync-' + origin, enable_sasl_external:true})
    this.connection = this.container.connect();
    this.sender = this.connection.open_sender(SERVICE_SYNC_ADDRESS);
    this.receiver = this.connection.open_receiver(SERVICE_SYNC_ADDRESS);
    bind(this, 'message');
    this.sender.send({subject:'service-sync-request', body:''});

    this.by_origin = {};
    this.by_name = {};
};

ServiceSync.prototype.reschedule = function () {
    if (this.scheduled) {
        clearTimeout(this.scheduled);
    }
    this.scheduled = setTimeout(function (self) {
        self.scheduled = undefined;//don't need to clear it after it fires
        self.send_local();
    }, 5000, this);
};

ServiceSync.prototype.send_local = function () {
    this.sender.send({subject:'service-sync-update', application_properties:{'origin': this.origin}, body:JSON.stringify(this.local)});
    this.reschedule();
};

ServiceSync.prototype.on_message = function (context) {
    if (context.message.subject === 'service-sync-update') {
        if (context.message.application_properties.origin !== this.origin) {
            var def;
            try {
                def = JSON.parse(context.message.body);
            } catch (e) {
                log.info('Failed to parse service-sync-update from %s: %s %s', context.message.application_properties.origin, context.message.body, e);
            }
            if (def) {
                log.debug('Received service-sync-update from %s: %j', context.message.application_properties.origin, def);
                var indexed = {};
                for (var i in def) {
                    indexed[def[i].address] = def[i];
                }
                this.ensure_definitions(context.message.application_properties.origin, indexed);
            }
        }
    } else if (context.message.subject === 'service-sync-request') {
        if (this.local === undefined) {
            log.info('Received service-sync-request but local definitions not yet loaded');
        } else {
            log.info('Received service-sync-request, sending local definitions');
            this.send_local();
        }
    }
};

ServiceSync.prototype.definitions_updated = function (definitions) {
    var latest = [];
    this.by_origin = {};
    this.by_name = {};
    for (var name in definitions) {
        var original = definitions[name];
        var service = {
            address: original.address,
            protocol: original.protocol,
            port: original.port,
            origin: original.origin,
            headless: original.headless,
            eventchannel: original.eventchannel,
            aggregate: original.aggregate,
            // don't propagate targets, which are only relevant locally
            targets: []
        };
        if (service.origin && service.origin !== 'annotation') {
            var o = this.by_origin[service.origin];
            if (o === undefined) {
                o = {};
                this.by_origin[service.origin] = o;
            }
            o[name] = service;
        } else {
            latest.push(service);
        }
        this.by_name[service.address] = service;
    }
    latest.sort(address_compare);
    var changes = myutils.changes(this.local, latest, address_compare, equivalent_service_record);
    if (changes) {
        this.local = latest;
        log.info('Local service definitions changed: %s', changes.description);
        this.send_local();
    }
};

ServiceSync.prototype.ensure_definitions = function (origin, service_defs) {
    var changed = [];//records to be added or updated
    var deleted = [];//names of records to be deleted
    for (var name in service_defs) {
        var service = service_defs[name];
        service.origin = origin;
        var existing = this.by_name[name];
        if (existing === undefined || (existing.origin === origin && !equivalent_service_record(service, existing))) {
            changed.push(service);
        }
    }
    if (this.by_origin && this.by_origin[origin]) {
        var current = this.by_origin[origin];
        for (var name in current) {
            if (service_defs[name] === undefined) {
                deleted.push(name);
            }
        }
    }
    service_utils.update_skupper_services(changed, deleted, origin);
};

function ensure_ports(service, ports) {
    if (equivalent_ports(ports, service.spec.ports)) {
        return false;
    } else {
        service.spec.ports = ports;
        return true;
    }
}

module.exports = function (origin) {
    return new ServiceSync(origin);
};
