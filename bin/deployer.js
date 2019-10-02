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

var kubernetes = require('../lib/kubernetes.js').client();
var labels = require("../lib/labels.js");
var log = require("../lib/log.js").logger();
var owner_ref = require("../lib/owner_ref.js");
var service_utils = require("../lib/service_utils.js");
var service_sync = require("../lib/service_sync.js");

const DEPLOYMENT = {
    group: 'apps',
    version: 'v1',
    name: 'deployments',
};

function Deployer(service_account, service_sync_origin) {
    this.service_account = service_account;
    this.service_watcher = kubernetes.watch('services');
    this.service_watcher.on('updated', this.services_updated.bind(this));
    this.deployment_watcher = kubernetes.watch(DEPLOYMENT, undefined, labels.TYPE_PROXY);
    this.deployment_watcher.on('updated', this.deployments_updated.bind(this));
    if (service_sync_origin) {
        this.service_sync = service_sync(service_sync_origin);
        this.service_watcher.on('updated', this.service_sync.updated.bind(this.service_sync));
    }
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

function get_network_name() {
    return process.env.SKUPPER_CONNECT_SECRET || 'skupper';
}

Deployer.prototype.services_updated = function (services) {
    log.info('services updated: %j', services.map(function (s) { return s.metadata.name; }));
    //verify services with proxy annotation set have proxies deployed
    services.filter(has_proxy_annotation).forEach(this.verify_deployment.bind(this));
};

Deployer.prototype.deployments_updated = function (deployments) {
    log.info('proxy deployments updated');
    //ensure proxy deployments still match service definitions
    deployments.forEach(this.verify_matches_service.bind(this));
};

Deployer.prototype.verify_deployment = function (service) {
    var self = this;
    log.info('Verifying proxy deployment for service %s', service.metadata.name);
    //ensure a proxy is deployed for the service and the service is
    //correctly configured for it
    var deployment_name = get_deployment_name(service);
    kubernetes.get(DEPLOYMENT, deployment_name).then(function (deployment) {
        self.reconcile(service, deployment);
    }).catch(function (code, description) {
        if (code === 404) {
            // deployment does not yet exist, deploy it
            log.info('Creating proxy deployment for %s', service.metadata.name);
            try {
                self.deploy(service);
                log.info('Proxy deployment created for %s', service.metadata.name);
            } catch (error) {
                log.error('Error deploying proxy for service: %s', error);
            }
        } else {
            log.error('Failed to retrieve proxy deployment: %s %s', deployment_name, code, description);
        }
    });
};

Deployer.prototype.reconcile = function (service, deployment) {
    var proxy = service.metadata.annotations[labels.PROXY];
    if (proxy === undefined || proxy.match(/none/i)) {
        this.undeploy(service, deployment);
    } else {
        var update = false;
        if (!this.verify_proxy_config(service, deployment)) {
            log.info('proxy config changed for deployment %s', deployment.metadata.name);
            update = true;
        }
        if (!this.verify_proxy_pod_selector(service, deployment)) {
            log.info('proxy pod selector changed for deployment %s', deployment.metadata.name);
            update = true;
        }

        if (update) {
            log.info('Updating proxy deployment %s', deployment.metadata.name);
            kubernetes.put(DEPLOYMENT, deployment).then(function () {
                log.info('Updated proxy deployment %s', deployment.metadata.name);
            }).catch(function (code, error) {
                log.error('Failed to update proxy deployment for %s: %s %s', deployment.metadata.name, code, error);
            });
        } else {
            log.info('No update required for proxy deployment %s', deployment.metadata.name);
        }
    }

    //check selector on service is as expected
    if (!this.verify_proxy_selector(service, deployment)) {
        log.info('Updating service selector for %s', service.metadata.name);
        service_utils.set_last_applied(service);
        kubernetes.put('services', service).then(function () {
            log.info('Updated service selector for %s', service.metadata.name);
        }).catch(function (code, error) {
            log.error('Failed to update service selector for %s: %s %s', service.metadata.name, code, error);
        });
    }

}

Deployer.prototype.verify_matches_service = function (deployment) {
    log.info('Verifying that service matches deployment %s', deployment.metadata.name);
    var self = this;
    var service_name = deployment.metadata.annotations[labels.SERVICE];
    if (service_name === undefined) {
        log.error('Annotation %s not found on deployment %s', labels.SERVICE, deployment.metadata.name);
    } else {
        kubernetes.get('services', service_name).then(function (service) {
            self.reconcile(service, deployment);
        }).catch(function (code, description) {
            if (code === 404) {
                // service no longer exists, just delete proxy deployment
                self.kubernetes.delete_(DEPLOYMENT, deployment.metadata.name).then(function () {
                    log.info('Deleted proxy deployment %s', deployment.metadata.name);
                }).catch(function (code, error) {
                    log.error('Failed to delete proxy deployment for %s: %s %s', deployment.metadata.name, code, error);
                });
            } else {
                log.error('Failed to retrieve service %s to verify proxy deployment: %s %s', service_name, code, description);
            }
        });
    }
};

function equivalent_selector (a, b) {
    for (var k in a) {
        if (b[k] !== a[k]) return false;
    }
    for (var k in b) {
        if (b[k] !== a[k]) return false;
    }
    return true;
}

//verify that the service selector matches the deployment (which will be the proxy deployment)
Deployer.prototype.verify_proxy_selector = function (service, deployment) {
    if (equivalent_selector(service.spec.selector, deployment.spec.selector.matchLabels)) {
        return true;
    } else {
        service.metadata.annotations[labels.ORIGINAL_SELECTOR] = stringify_selector(service.spec.selector);
        service.spec.selector = deployment.spec.selector.matchLabels;
        return false;
    }
};

//verify that the proxy is targetting the right set of pods
Deployer.prototype.verify_proxy_pod_selector = function (service, deployment) {
    var desired = service.metadata.annotations[labels.ORIGINAL_SELECTOR];
    if (desired === undefined) {
        if (!equivalent_selector(service.spec.selector, deployment.spec.selector.matchLabels)) {
            desired = stringify_selector(service.spec.selector);
        } else {
            log.error('Cannot determine correct pod selector for proxy from service %s, deployment %s', service.metadata.name, deployment.metadata.name);
            return true;//can't update deployment
        }
    }
    var actual = get_container_env_value(deployment, 'proxy', 'ICPROXY_POD_SELECTOR');
    if (actual === desired) {
        return true;
    } else {
        set_container_env_value(deployment, 'proxy', 'ICPROXY_POD_SELECTOR', desired)
        return false;
    }
};

function equivalent_proxy_config (a, b) {
    return a.sort().join(',') === b.sort().join(',');
}

function get_proxy_config (service) {
    //TODO: support different protocols on different ports
    var protocol = service.metadata.annotations[labels.PROXY];
    var address = service.metadata.annotations[labels.ADDRESS] || service.metadata.name;
    var bridges = [];
    for (var i in service.spec.ports) {
        var port = service.spec.ports[i];
        var incoming = protocol + ":" + port.targetPort + "=>amqp:" + address;
        var outgoing = "amqp:" + address + "=>" + protocol + ":" + port.targetPort;
        bridges.push(incoming);
        bridges.push(outgoing);
    }
    return bridges;
};

Deployer.prototype.verify_proxy_config = function (service, deployment) {
    var desired = get_proxy_config(service);
    var config = get_container_env_value(deployment, 'proxy', 'ICPROXY_CONFIG');
    if (config && equivalent_proxy_config(desired, config.split(','))) {
        return true;
    } else {
        set_container_env_value(deployment, 'proxy', 'ICPROXY_CONFIG', desired.join(','))
        log.info('updated proxy config for %s: %j', deployment.metadata.name, desired);
        return false;
    }
};

function stringify_selector (selector) {
    var elements = [];
    for (var k in selector) {
        elements.push(k + '=' + selector[k]);
    }
    return elements.join(',');
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

Deployer.prototype.undeploy = function (service, deployment) {
    log.info('Restoring service %s', service.metadata.name);
    // get original selector, update service to use that
    var value = get_container_env_value(deployment, 'proxy', 'ICPROXY_POD_SELECTOR');
    if (value === undefined) {
        log.error('Could not find original selector from deployment %s', deployment.metadata.name);
        return;
    }
    var selector = selector_from_string(value);
    log.info('Restoring selector for %s', service.metadata.name);
    kubernetes.update('services', service.metadata.name, function (original) {
        original.spec.selector = selector;
        service_utils.set_last_applied(original);
        return original;
    }).then(function (result) {
        var code = result.code;
        if (is_success_code(code)) {
            log.info('Restored selector for %s', service.metadata.name);
            log.info('Deleting deployment %s', deployment.metadata.name);
            kubernetes.delete_(DEPLOYMENT, deployment.metadata.name).then(function (code, description) {
                if (is_success_code(code)) {
                    log.info('Deleted deployment %s', deployment.metadata.name);
                } else {
                    log.error('Failed to delete deployment %s: %s %s', deployment.metadata.name, code, description);
                }
            }).catch(function (error) {
                log.error('Failed to delete deployment %s: %s', deployment.metadata.name, error);
            });
        } else {
            log.error('Failed to restore selector for %s: %s %j', service.metadata.name, code, result);
        }
    }).catch(function (code, error) {
        log.error('Failed to restore selector for %s: %s %s', service.metadata.name, code, error);
    });
};

function get_proxy_selector(service) {
    var o = {};
    o[labels.SERVICE] = service.metadata.name;
    return o;
}

Deployer.prototype.deploy = function (service) {
    var original_selector = stringify_selector(service.spec.selector);
    // deploy the proxy
    var deployment = {
        metadata: {
            name: get_deployment_name(service),
            annotations : {},
        },
        spec: {
            selector: {
                matchLabels: get_proxy_selector(service),
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
                                name: 'ICPROXY_CONFIG',
                                value: get_proxy_config(service).join(',')
                            },
                            {
                                name: 'ICPROXY_POD_SELECTOR',
                                value: original_selector
                            }
                        ],
                        image: 'quay.io/skupper/icproxy',
                        volumeMounts: [{
                            name: 'connect',
                            mountPath: "/etc/messaging/"
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
    deployment.metadata.annotations[labels.SERVICE] = service.metadata.name;
    deployment.spec.template.metadata.labels[labels.SERVICE] = service.metadata.name;
    log.info('Deploying proxy for %s', service.metadata.name);
    kubernetes.post(DEPLOYMENT, deployment).then(function (code, description) {
        if (is_success_code(code)) {
            log.info('Deployed proxy for %s', service.metadata.name);
            // change the selector
            log.info('Updating selector for %s', service.metadata.name);
            kubernetes.update('services', service.metadata.name, function (original) {
                original.spec.selector = get_proxy_selector(service);
                original.metadata.annotations[labels.ORIGINAL_SELECTOR] = original_selector;
                service_utils.set_last_applied(original);
                return original;
            }).then(function (result) {
                var code = result.code;
                if (is_success_code(code)) {
                    log.info('Updated selector for %s', service.metadata.name);
                } else {
                    log.error('Failed to update selector for %s: %s %j', service.metadata.name, code, result);
                }
            }).catch(function (code, error) {
                log.error('Failed to update selector for %s: %s %s', service.metadata.name, code, error);
            });
        } else {
            log.error('Failed to deploy proxy for %s: %s %s %j', service.metadata.name, code, description, deployment);
        }
    }).catch(function (code, error) {
        log.error('Failed to deploy proxy for %s: %s %s', service.metadata.name, code, error);
        console.error(error);
    });

};

var deployer = new Deployer(process.env.ICPROXY_SERVICE_ACCOUNT || 'icproxy', process.env.SKUPPER_SERVICE_SYNC_ORIGIN);
