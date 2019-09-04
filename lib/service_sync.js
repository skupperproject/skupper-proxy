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
var service_utils = require('./service_utils.js');

const SERVICE_SYNC_ADDRESS="mc/$skupper-service-sync"

function is_local_proxied (service) {
    return service.metadata.annotations && service.metadata.annotations[labels.PROXY] && service.metadata.annotations[labels.ORIGIN] === undefined;
}

function to_service_record (service) {
    var record = {
        name: service.metadata.name,
        proxy: service.metadata.annotations[labels.PROXY],
        ports: service.spec.ports
    };
    if (service.metadata.annotations[labels.ADDRESS]) {
        record.address = service.metadata.annotations[labels.ADDRESS];
    }
    return record;
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

function equivalent_service_record (a, b) {
    return a.name === b.name && a.proxy === b.proxy && a.address === b.address && equivalent_ports(a.ports, b.ports);
}

function name_compare (a, b) {
    return myutils.string_compare(a.name, b.name);
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
};

ServiceSync.prototype.send_local = function () {
    this.sender.send({subject:'service-sync-update', application_properties:{'origin': this.origin}, body:this.local});
};

ServiceSync.prototype.on_message = function (context) {
    if (context.message.subject === 'service-sync-update') {
        if (context.message.application_properties.origin !== this.origin) {
            log.info('Received service-sync-update from %s: %j', context.message.application_properties.origin, context.message.body);
            this.reconcile(context.message.application_properties.origin, context.message.body);
        }
    } else if (context.message.subject === 'service-sync-request') {
        log.info('Received service-sync-request');
        this.send_local();
    }
};

ServiceSync.prototype.updated = function (services) {
    var latest = services.filter(is_local_proxied).map(to_service_record);
    latest.sort(name_compare);
    var changes = myutils.changes(this.local, latest, name_compare, equivalent_service_record);
    if (changes) {
        this.local = latest;
        log.info('Local service definitions changed: %s', changes.description);
        this.send_local();
    }
};

ServiceSync.prototype.reconcile = function (origin, service_defs) {
    Promise.all(service_defs.map(reconcile_service(origin))).then(function () {
        //TODO: need to delete any services that were removed at origin

        log.info('Services synced');
    }).catch(function (error) {
        console.error('Error syncing services: %s', error);
    });
};

function ensure_annotation(service, name, value) {
    if (service.metadata.annotations) {
        if (service.metadata.annotations[name] === value) {
            return false; //no update needed
        } else {
            if (value) {
                service.metadata.annotations[name] = value;
            } else {
                delete service.metadata.annotations[name];
            }
            return true;
        }
    } else if (value) {
        service.metadata.annotations = {name: value};
        return true;
    }
}

function ensure_ports(service, ports) {
    if (equivalent_ports(ports, service.spec.ports)) {
        return false;
    } else {
        service.spec.ports = ports;
        return true;
    }
}

function service_update_function (origin, def) {
    return function (service) {
        if (service === undefined) {
            log.info('service-sync creating service for %j', def);
            service = {
                apiVersion: 'v1',
                kind: 'Service',
                metadata: {
                    name: def.name,
                    annotations: {}
                },
                spec: {
                    ports: def.ports,
                    selector: {'skupper.io/implements':def.name}
                }
            };
            service.metadata.annotations[labels.PROXY] = def.proxy;
            service.metadata.annotations[labels.ORIGIN] = origin;
            if (def.address) {
                service.metadata.annotations[labels.ADDRESS] = def.address;
            }
            service_utils.set_last_applied(service);
            return service;
        } else {
            var changed = ensure_annotation(service, labels.PROXY, def.proxy);
            changed = ensure_annotation(service, labels.ADDRESS, def.address) || changed;
            changed = ensure_ports(service, def.ports) || changed;
            if (changed) {
                service_utils.set_last_applied(service);
                log.info('service-sync updating service for %j', def);
                return service;
            } else {
                log.debug('Service has not changed %j', def);
                return undefined;
            }
        }
    };
};

function reconcile_service (origin) {
    return function (def) {
        return kubernetes.update('services', def.name, service_update_function(origin, def));
    }
}

module.exports = function (origin) {
    return new ServiceSync(origin);
};
