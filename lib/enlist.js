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

var kubernetes = require('./kubernetes.js').client();
var labels = require('./labels.js');
var log = require('./log.js').logger();
var myutils = require('./utils.js');
var owner_ref = require("../lib/owner_ref.js");
var service_utils = require('./service_utils.js');

function is_success_code(code) {
    return code >= 200 && code < 300;
}

function has_proxy_annotation (service) {
    return service.metadata.annotations && service.metadata.annotations[labels.PROXY];
}

function extract_service_port (port) {
    var result = {};
    ['name', 'port', 'protocol', 'targetPort'].forEach(function (field) {
        if (port[field]) result[field] = port[field];
    });
    return result;
}

function to_service_record (service) {
    var record = {
        name: service.metadata.name,
        proxy: service.metadata.annotations[labels.PROXY],
        ports: service.spec.ports.map(extract_service_port)
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

function equivalent_target (a, b) {
    return a.name === b.name && a.weight === b.weight && a.targetPort === b.targetPort && a.selector === b.selector && a.filter === b.filter;
}

function equivalent_targets (a, b) {
    a.sort(name_compare);
    b.sort(name_compare);
    return myutils.changes(a, b, name_compare, equivalent_target) === undefined;
}

function compatible_service_records (a, b) {
    return a.address === b.address && a.protocol === b.protocol && a.port === b.port;
}

function equivalent_service_record (a, b) {
    return compatible_service_records(a, b) && equivalent_targets(a.targets, b.targets);
}

function name_compare (a, b) {
    return myutils.string_compare(a.name, b.name);
}

function Enlist () {
    this.started = false;
};

Enlist.prototype.start = function () {
    this.deployment_watcher = kubernetes.watch(kubernetes.DEPLOYMENT);
    this.deployment_watcher.on('updated', this.deployments_updated.bind(this));
    this.service_watcher = kubernetes.watch('services');
    this.service_watcher.on('updated', this.services_updated.bind(this));
};

Enlist.prototype.definitions_updated = function (definitions) {
    this.from_annotation = {};
    this.by_name = {};
    for (var name in definitions) {
        var service = definitions[name];
        if (service.origin === 'annotation') {
            this.from_annotation[service.address] = service;
        }
        this.by_name[service.address] = service;
    }
    if (!this.started) {
        this.started = true;
        this.start();
    }
};

function stringify_selector (selector) {
    var elements = [];
    for (var k in selector) {
        elements.push(k + '=' + selector[k]);
    }
    return elements.join(',');
}

function service_to_service_record(service) {
    var port = {};
    if (service.spec.ports && service.spec.ports.length > 0) {
        port = service.spec.ports[0];
    } else {
        log.error('Could not extract port from service %s: spec.ports=%j', service.metadata.name, service.spec.ports);
        return;
    }
    var record = {
        address: service.metadata.name,
        protocol: service.metadata.annotations[labels.PROXY],
        //TODO: handle multiple ports?
        port: port.port,
        targets: [
            {
                selector: stringify_selector(service.spec.selector)
            }
        ]
    };
    if (port.targetPort) {
        record.targets[0].targetPort = port.targetPort;
    }
    return record;
}

function deployment_to_service_record(deployment) {
    var port = 0;
    if (deployment.spec.template.spec.containers && deployment.spec.template.spec.containers.length) {
        //TODO: handle multiple containers?
        var container = deployment.spec.template.spec.containers[0];
        if (container.ports && container.ports.length) {
            //TODO: handle multiple ports?
            port = container.ports[0].containerPort;
        }
    }
    var record = {
        address: deployment.metadata.annotations[labels.ADDRESS] || deployment.metadata.name,
        protocol: deployment.metadata.annotations[labels.PROXY],
        port: port,
        targets: [
            {
                name: deployment.metadata.annotations[labels.VERSION] || deployment.metadata.name,
                selector: stringify_selector(deployment.spec.selector.matchLabels)
            }
        ]
    };
    if (deployment.metadata.annotations[labels.WEIGHT]) {
        var weight = Number(deployment.metadata.annotations[labels.WEIGHT]);
        if (weight === NaN) {
            log.error('%s annotation on deployment %s must be a number', labels.WEIGHT, deployment.metadata.name);
        } else {
            record.targets[0].weight = weight;
        }
    }
    return record;
}

function merge_targets(current, added_service_definition) {
    if (added_service_definition.targets && added_service_definition.targets.length == 1) {
        var added_target = added_service_definition.targets[0];
        if (added_service_definition.port && added_service_definition.port !== current.port) {
            added_target.targetPort = added_service_definition.port;
        }
        var result = {
            address: current.address,
            protocol: current.protocol,
            port: current.port,
            targets: []
        }
        var modified = false;
        for (var i in current.targets) {
            var target = current.targets[i];
            if (target.name === added_target.name) {
                if (!equivalent_target(target, added_target)) {
                    result.targets.push(added_target);
                    modified = true;
                    log.info('updating target %s for service %s', target.name, current.address);
                } else {
                    log.info('target %s for service %s is up to date', target.name, current.address);
                    return undefined;
                }
            } else {
                result.targets.push(target);
            }
        }
        if (!modified) {
            result.targets.push(added_target);
        }
        return result;
    } else {
        return undefined;
    }
}

Enlist.prototype.update_services = function (records) {
    var changed = [];//records to be added or updated
    var deleted = [];//names of records to be deleted
    var annotated = {};
    for (var i in records) {
        var service = records[i];
        var existing = this.by_name[service.address];
        if (existing === undefined) {
            changed.push(service);
        } else if (existing.protocol === service.protocol) {
            var merged = merge_targets(existing, service);
            if (merged) {
                changed.push(merged);
            }
        } else {
            log.error('Invalid protocol %s, address %s already proxied for %s', service.protocol, service.address, existing.protocol);
        }
    }
    for (var name in this.from_annotation) {
        if (annotated[name] === undefined) {
            deleted.push(name);
        }
    }
    service_utils.update_skupper_services(changed, deleted, 'annotated');
};

Enlist.prototype.services_updated = function (services) {
    if (this.by_name === undefined) return;
    this.update_services(services.filter(has_proxy_annotation).map(service_to_service_record));
};

Enlist.prototype.deployments_updated = function (deployments) {
    if (this.by_name === undefined) return;
    this.update_services(deployments.filter(has_proxy_annotation).map(deployment_to_service_record));
};

module.exports = function () {
    return new Enlist();
};
