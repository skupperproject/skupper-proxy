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

var fs = require('fs');
var path = require('path');

var enlist = require('../lib/enlist.js');
var kubernetes = require('../lib/kubernetes.js').client();
var labels = require('../lib/labels.js');
var log = require('../lib/log.js').logger();
var metrics = require('../lib/metrics.js');
var owner_ref = require('../lib/owner_ref.js');
var scaling = require('../lib/statefulset_scaling.js')();
var service_utils = require('../lib/service_utils.js');
var service_sync = require('../lib/service_sync.js');

function auth(dir) {
    if (dir) {
        return function (username, password) {
            return new Promise (function (resolve, reject) {
                fs.readFile(path.resolve(dir, username), 'utf8', function (err, data) {
                    if (err) {
                        log.info('Error authenticating %s: %s', username, err)
                    }
                    resolve(!err && data.toString() === password);
                });
            });
        };
    } else {
        return undefined;
    }
}

function Deployer(service_account, service_sync_origin) {
    this.service_account = service_account;
    this.origin = service_sync_origin;

    this.service_watcher = kubernetes.watch('services');
    this.service_watcher.on('updated', this.services_updated.bind(this));

    this.deployment_watcher = kubernetes.watch(kubernetes.DEPLOYMENT, undefined, labels.TYPE_PROXY);
    this.deployment_watcher.on('updated', this.deployments_updated.bind(this));
    this.statefulset_watcher = kubernetes.watch(kubernetes.STATEFULSET, undefined, labels.TYPE_PROXY);
    this.statefulset_watcher.on('updated', this.statefulsets_updated.bind(this));
    this.config_watcher = kubernetes.watch_resource('configmaps', 'skupper-services');
    this.config_watcher.on('updated', this.definitions_updated.bind(this));
    if (service_sync_origin) {
        this.service_sync = service_sync(service_sync_origin);
    }
    if (!process.env.DISABLE_ENLIST_FROM_ANNOTATIONS) {
        this.enlist = enlist();
    }
    this.metrics_server = metrics.create_server(this.origin, process.env.METRICS_PORT || 8080, process.env.METRICS_HOST, auth(process.env.METRICS_USERS));
}

function is_success_code(code) {
    return code >= 200 && code < 300;
}

function has_proxy_annotation (service) {
    return service.metadata.annotations && service.metadata.annotations[labels.PROXY];
}

function get_deployment_name(service) {
    return service.metadata.name + '-proxy';
}

function get_proxy_name(service_name) {
    return service_name + '-proxy';
}

function get_service_name(proxy_name) {
    var i = proxy_name.indexOf('-proxy');
    if (i > 0) {
        return proxy_name.substring(0, i);
    } else {
        return proxy_name;
    }
}

function get_network_name() {
    return process.env.SKUPPER_CONNECT_SECRET || 'skupper';
}

function is_owned(service) {
    return service.metadata.annotations && service.metadata.annotations[labels.CONTROLLED] && owner_ref.is_owner(service);
}

function has_original_selector(service) {
    return service.metadata.annotations && service.metadata.annotations[labels.ORIGINAL_SELECTOR];
}

Deployer.prototype.process_annotated_services = function (services) {
    //verify services with proxy annotation set have proxies deployed
    services.filter(has_proxy_annotation).forEach(this.verify_deployment.bind(this));
};

Deployer.prototype.services_updated = function (services) {
    log.info('services updated: %j', services.map(function (s) { return s.metadata.name; }));
    this.actual_services = services.reduce(function (a, b) {
        a[b.metadata.name] = b;
        return a;
    }, {});
    this.reconcile();
};

Deployer.prototype.deployments_updated = function (deployments) {
    this.proxies = deployments.reduce(function (a, b) {
        a[b.metadata.name] = b;
        return a;
    }, {});
    log.info('proxy deployments updated: %j', Object.keys(this.proxies));
    //ensure proxy deployments still match service definitions
    this.reconcile();
};

Deployer.prototype.statefulsets_updated = function (deployments) {
    this.ss_proxies = deployments.reduce(function (a, b) {
        a[b.metadata.name] = b;
        return a;
    }, {});
    log.info('proxy statefulsets updated: %j', Object.keys(this.ss_proxies));
    this.reconcile();
};

Deployer.prototype.definitions_updated = function (config) {
    if (config && config.length == 1) {
        var definitions = {};
        for (var name in config[0].data) {
            try {
                definitions[name] = JSON.parse(config[0].data[name]);
            } catch (e) {
                log.error('Could not parse service definition for %s as JSON: e', name, e);
            }
        }
        this.desired_services = definitions;
        log.info('Desired service configuration updated: %j', this.desired_services);
        this.reconcile();
        if (this.service_sync) {
            this.service_sync.definitions_updated(definitions);
        }
        if (this.enlist) {
            this.enlist.definitions_updated(definitions);
        }
        scaling.definitions_updated(definitions);
    } else {
        this.desired_services = {};
        log.info('No skupper services defined.');
        this.reconcile();
        if (this.service_sync) {
            this.service_sync.definitions_updated({});
        }
        if (this.enlist) {
            this.enlist.definitions_updated({});
        }
    }
};

Deployer.prototype.ensure_proxy_for = function (name) {
    var service = this.desired_services[name];
    var proxy = this.proxies[get_proxy_name(name)];
    if (service.headless) {
        var proxy_name = service.origin ? service.headless.name : get_proxy_name(name);
        var ss_proxy = this.ss_proxies[proxy_name];
        if (ss_proxy === undefined) {
            log.info('Need to create proxy for %s as statefulset', name);
            this.deploy_as_statefulset(name, service);
        } else {
            log.info('Checking existing statefulset proxy for %s: %j', name, ss_proxy);
            //TODO: any other checks required?
            var config_changed = !equivalent_proxy_config(service, ss_proxy);
            var size_changed = service.headless.size != ss_proxy.spec.replicas;
            if (config_changed || size_changed) {
                log.info('Need to update proxy for %s (%j)', name, service);
                kubernetes.update(kubernetes.STATEFULSET, proxy_name, function (original) {
                    if (config_changed) {
                        log.info('proxy config needs changed for %s', name);
                        set_container_env_value(original, 'proxy', 'SKUPPER_PROXY_CONFIG', JSON.stringify(service));
                    }
                    if (size_changed) {
                        log.info('proxy replicas needs changed for %s', name);
                        original.spec.replicas = service.headless.size;
                    }
                    return original;
                }).then(function (result) {
                    if (is_success_code(result.code)) {
                        log.info('Updated proxy for %s', name);
                    } else{
                        log.info('Could not update proxy for %s: %s', name, result.code);
                    }
                }).catch(console.error);
            }
        }
    } else {
        if (proxy === undefined) {
            log.info('Need to create proxy for %s (%j)', name, service);
            this.deploy(name, service);
        } else {
            //TODO: any other checks required?
            if (!equivalent_proxy_config(service, proxy)) {
                log.info('Need to update proxy config for %s (%j)', name, service);
                kubernetes.update(kubernetes.DEPLOYMENT, get_proxy_name(name), function (original) {
                    set_container_env_value(original, 'proxy', 'SKUPPER_PROXY_CONFIG', JSON.stringify(service))
                    return original;
                }).then(function (result) {
                    if (is_success_code(result.code)) {
                        log.info('Updated proxy config for %s', name);
                    } else{
                        log.info('Could not update proxy config for %s: %s', name, result.code);
                    }
                }).catch(console.error);
            }
        }
    }
};

Deployer.prototype.ensure_service_for = function (name) {
    log.info('Checking service for %s', name);
    var desired = this.desired_services[name];
    var actual = this.actual_services[name];
    if (desired && desired.headless) {
        if (desired.origin) {
            // ensure headless service
            if (actual === undefined) {
                this.create_service(name, desired);
            } else {
                var selector = get_proxy_selector(name);
                if (actual.spec.clusterIP != 'None') {
                    this.recreate_service(name, desired);
                } else if (actual.spec.ports[0].port !== desired.port || !equivalent_selector(actual.spec.selector, selector)) {
                    this.update_service(name, desired);
                } else {
                    log.info('Service for %s already defined', name);
                }
            }
        } // else assume statefulset is local and service is already defined
    } else {
        if (actual === undefined) {
            this.create_service(name, desired);
        } else {
            var selector = get_proxy_selector(name);
            if (actual.spec.ports[0].port !== desired.port || !equivalent_selector(actual.spec.selector, selector)) {
                if (actual.spec.ports[0].port !== desired.port) {
                    log.info('Port does not match for service %s: expected %j, have %j', name, desired.port, actual.spec.ports[0].port);
                }
                if (!equivalent_selector(actual.spec.selector, selector)) {
                    log.info('Selector does not match for service %s: expected %j, have %j', name, selector, actual.spec.selector);
                }
                if (is_owned(actual) || desired.clobber) {
                    log.info('Updating service for %s %s', name, desired.clobber ? '(clobber enabled)' : '');
                    this.update_service(name, desired);
                } else {
                    log.warn('Service %s already exists with different configuration');
                }
            } else {
                log.info('Service for %s already defined', name);
            }
        }
    }
};

Deployer.prototype.delete_service = function (name) {
    kubernetes.delete_('services', name).then(function () {
        log.info('Deleted service %s', name);
    }).catch(console.error);
};

Deployer.prototype.delete_proxy = function (name) {
    kubernetes.delete_(kubernetes.DEPLOYMENT, name).then(function () {
        log.info('Deleted proxy %s', name);
    }).catch(console.error);
};

Deployer.prototype.reconcile = function () {
    if (this.desired_services !== undefined && this.actual_services !== undefined && this.proxies !== undefined && this.ss_proxies !== undefined) {
        console.log('Reconciling...');
        //reconcile proxy deployments with desired services:
        for (var name in this.desired_services) {
            this.ensure_proxy_for(name);
        }
        for (var name in this.proxies) {
            if (this.desired_services[get_service_name(name)] === undefined) {
                log.info('Undeploying proxy %s', name);
                this.delete_proxy(name);
            }
        }

        //reconcile actual services with desired services:
        for (var name in this.desired_services) {
            this.ensure_service_for(name);
        }
        for (var name in this.actual_services) {
            if (this.desired_services[name] === undefined) {
                var actual = this.actual_services[name];
                if (is_owned(actual)) {
                    log.info('Deleting service %s', name);
                    this.delete_service(name);
                } else if (has_original_selector(actual)) {
                    this.restore_service(actual);
                }
            }
        }
    } else {
        if (!this.desired_services) log.info('Reconciliation pending; desired service configuration not yet loaded');
        if (!this.actual_services) log.info('Reconciliation pending; actual service definitions not yet loaded');
        if (!this.proxies) log.info('Reconciliation pending; proxy deployments not yet loaded');
        if (!this.ss_proxies) log.info('Reconciliation pending; proxy statefulsets not yet loaded');
    }
};

function equivalent_selector (a, b) {
    if (a === undefined || b === undefined) return false;

    for (var k in a) {
        if (b[k] !== a[k]) return false;
    }
    for (var k in b) {
        if (b[k] !== a[k]) return false;
    }
    return true;
}

function equivalent_proxy_config (desired, deployment) {
    var config = get_container_env_value(deployment, 'proxy', 'SKUPPER_PROXY_CONFIG');
    return config && JSON.stringify(config) === JSON.stringify(desired);
}

function selector_from_string (selector) {
    var elements = selector.split(',');
    var result = {};
    for (var i in elements) {
        var parts = elements[i].split('=');
        result[parts[0]] = parts[1];
    }
    return result;
}

function selector_to_string (selector) {
    var result;
    for (var key in selector) {
        var element = key + '=' + selector[key];
        if (result === undefined) {
            result = element;
        } else {
            result += ',' + element;
        }
    }
    return result;
}

function get_container(deployment, name) {
    for (var i in deployment.spec.template.spec.containers) {
        var container = deployment.spec.template.spec.containers[i];
        if (container.name === name) return container;
    }
    log.error('Could not find container %s in deployment %s', container_name, deployment.metadata.name);
    return undefined;
}

function get_env_value(env, key) {
    for (var i in env) {
        if (env[i].name === key) {
            return env[i].value;
        }
    }
    return undefined;
}

function set_env_value(env, key, value) {
    for (var i in env) {
        if (env[i].name === key) {
            env[i].value = value;
            return;
        }
    }
    env.push({name: key, value: value});
}

function get_container_env_value(deployment, container_name, key) {
    var container = get_container(deployment, container_name);
    if (container) {
        return get_env_value(container.env, key);
    } else {
        return undefined;
    }
}

function set_container_env_value(deployment, container_name, key, value) {
    var container = get_container(deployment, container_name);
    if (container) {
        set_env_value(container.env, key, value);
    } else {
        log.error('Could not set env var %s on container %s: no such container in %s', key, container_name, deployment.metadata.name);
    }
}

function get_proxy_selector(service_name) {
    var o = {};
    o[labels.SERVICE] = service_name;
    return o;
}

Deployer.prototype.recreate_service = function (name, desired_service) {
    kubernetes.delete_('services', name).then(function (code, data) {
        if (is_success_code(code)) {
            log.info('Deleted service %s, will recreate', name);

            var service = {
                apiVersion: 'v1',
                kind: 'Service',
                metadata: {
                    name: name,
                    annotations: {}
                },
                spec: {
                    //TODO: consider support for multiple ports on same service?
                    ports: [
                        {
                            name: name,
                            port: desired_service.port,
                            targetPort: Math.max(desired_service.port, 1024)
                        }
                    ],
                    selector: get_proxy_selector(name)
                }
            };
            if (desired_service.headless) {
                service.spec.clusterIP = 'None';
            }
            service.metadata.annotations[labels.CONTROLLED] = 'true';
            owner_ref.set_owner_references(service);
            service_utils.set_last_applied(service);
            console.log('Recreating service %j', service);
            kubernetes.post('services', service).then(function (code, data) {
                if (is_success_code(code)) {
                    log.info('Created service for %s', name);
                } else {
                    log.error('Failed to create service for %s: %s %j', name, code, data);
                }
            }).catch(function (error) {
                log.error('Error while (re)creating service for %s: %s', name, error);
            });
        } else {
            log.error('Failed to create service for %s: %s %j', name, code, data);
        }
    }).catch(function (error) {
        log.error('Error while deleting service to be recreated %s: %s', name, error);
    });
};

Deployer.prototype.create_service = function (name, desired_service) {
    var service = {
        apiVersion: 'v1',
        kind: 'Service',
        metadata: {
            name: name,
            annotations: {}
        },
        spec: {
            //TODO: consider support for multiple ports on same service?
            ports: [
                {
                    name: name,
                    port: desired_service.port,
                    targetPort: Math.max(desired_service.port, 1024)
                }
            ],
            selector: get_proxy_selector(name)
        }
    };
    if (desired_service.headless) {
        service.spec.clusterIP = 'None';
    }
    service.metadata.annotations[labels.CONTROLLED] = 'true';
    owner_ref.set_owner_references(service);
    console.log('Creating service %j', service);
    kubernetes.post('services', service).then(function (code, data) {
        if (is_success_code(code)) {
            log.info('Created service for %s', name);
        } else {
            log.error('Failed to create service for %s: %s %j', name, code, data);
        }
    }).catch(function (error) {
        log.error('Error while creating service for %s: %s %s', name, error);
    });
};

function restore_service(service) {
    kubernetes.update('services', service.metadata.name, function (original) {
        original.spec.selector = selector_from_string(original.metadata.annotations[labels.ORIGINAL_SELECTOR]);
        delete original.metadata.annotations[labels.ORIGINAL_SELECTOR];
        return original;
    }).then(function (result) {
        var code = result.code;
        if (is_success_code(code)) {
            log.info('Updated service for %s', name);
        } else {
            log.error('Failed to update service for %s: %s %j', name, code, result);
        }
    }).catch(function (code, error) {
        log.error('Failed to update service for %s: %s %s', name, code, error);
    });
};

Deployer.prototype.update_service = function (name, desired_service) {
    kubernetes.update('services', name, function (original) {
        var desired_selector = get_proxy_selector(name);
        if (!equivalent_selector(original.spec.selector, desired_selector)) {
            original.metadata.annotations[labels.ORIGINAL_SELECTOR] = selector_to_string(original.spec.selector);
            original.spec.selector = desired_selector;
        }
        original.spec.ports[0].port = desired_service.port;
        original.spec.ports[0].targetPort = Math.max(desired_service.port, 1024);
        return original;
    }).then(function (result) {
        var code = result.code;
        if (is_success_code(code)) {
            log.info('Updated service for %s', name);
        } else {
            log.error('Failed to update service for %s: %s %j', name, code, result);
        }
    }).catch(function (code, error) {
        log.error('Failed to update service for %s: %s %s', name, code, error);
    });
};

Deployer.prototype.deploy = function (service_name, config) {
    // deploy the proxy
    var deployment = {
        metadata: {
            name: get_proxy_name(service_name),
            annotations : {},
            labels: {}
        },
        spec: {
            selector: {
                matchLabels: get_proxy_selector(service_name),
            },
            template : {
                metadata: {
                    labels: {},
                },
                spec: {
                    serviceAccountName: this.service_account,
                    containers: [{
                        name: 'proxy',
                        env: [
                            {
                                name: 'SKUPPER_PROXY_CONFIG',
                                value: JSON.stringify(config)
                            },
                            {
                                name: 'SKUPPER_SITE_ID',
                                value: this.origin
                            }
                        ],
                        image: process.env.SKUPPER_PROXY_IMAGE || 'quay.io/skupper/proxy',
                        volumeMounts: [{
                            name: 'connect',
                            mountPath: '/etc/messaging/'
                        }],
                    }],
                    volumes: [{
                        name: 'connect',
                        secret: {
                            secretName: get_network_name()
                        }
                    }]
                }
            }
        }
    };
    owner_ref.set_owner_references(deployment);
    deployment.metadata.labels[labels.TYPE] = 'proxy';
    deployment.metadata.annotations[labels.SERVICE] = service_name;
    deployment.spec.template.metadata.labels[labels.SERVICE] = service_name;
    log.info('Deploying proxy for %s', service_name);
    kubernetes.post(kubernetes.DEPLOYMENT, deployment).then(function (code, description) {
        if (is_success_code(code)) {
            log.info('Deployed proxy for %s', service_name);
        } else {
            log.error('Failed to deploy proxy for %s: %s %s', service_name, code, description);
        }
    }).catch(function (code, error) {
        log.error('Failed to deploy proxy for %s: %s %s', service_name, code, error);
        console.error(error);
    });

};

Deployer.prototype.deploy_as_statefulset = function (service_name, config) {
    // deploy the proxy
    var statefulset = {
        apiVersion: 'apps/v1',
        kind: 'StatefulSet',
        metadata: {
            name: config.origin ? config.headless.name : get_proxy_name(service_name),
            annotations : {},
            labels: {}
        },
        spec: {
            serviceName: service_name,
            replicas: config.headless.size,
            selector: {
                matchLabels: get_proxy_selector(service_name),
            },
            template : {
                metadata: {
                    labels: {},
                },
                spec: {
                    serviceAccountName: this.service_account,
                    containers: [{
                        name: 'proxy',
                        env: [
                            {
                                name: 'SKUPPER_PROXY_CONFIG',
                                value: JSON.stringify(config)
                            },
                            {
                                name: 'NAMESPACE',
                                valueFrom: {
                                    fieldRef: {
                                        fieldPath: 'metadata.namespace'
                                    }
                                }
                            }
                        ],
                        image: process.env.SKUPPER_PROXY_IMAGE || 'quay.io/skupper/proxy',
                        volumeMounts: [{
                            name: 'connect',
                            mountPath: '/etc/messaging/'
                        }],
                    }],
                    volumes: [{
                        name: 'connect',
                        secret: {
                            secretName: get_network_name()
                        }
                    }]
                }
            }
        }
    };
    owner_ref.set_owner_references(statefulset);
    statefulset.metadata.labels[labels.TYPE] = 'proxy';
    statefulset.metadata.annotations[labels.SERVICE] = service_name;
    statefulset.spec.template.metadata.labels[labels.SERVICE] = service_name;
    log.info('Deploying proxy for %s as statefulset: %j', service_name, statefulset);
    kubernetes.post(kubernetes.STATEFULSET, statefulset).then(function (code, description) {
        if (is_success_code(code)) {
            log.info('Deployed proxy for %s', service_name);
        } else {
            log.error('Failed to deploy proxy for %s: %s %s', service_name, code, description);
        }
    }).catch(function (code, error) {
        log.error('Failed to deploy proxy for %s: %s %s', service_name, code, error);
        console.error(error);
    });

};

process.on('SIGTERM', function () {
    console.log('Exiting due to SIGTERM');
    process.exit();
});
var deployer = new Deployer(process.env.SKUPPER_SERVICE_ACCOUNT || 'skupper', process.env.SKUPPER_SERVICE_SYNC_ORIGIN);
