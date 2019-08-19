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
var log = require("../lib/log.js").logger();

const BASE_QUALIFIER = "skupper.github.com";//TODO: replace with proper name
const PROXY_QUALIFIER = "proxy." + BASE_QUALIFIER;
const ADDRESS = PROXY_QUALIFIER + "/address";
const PROTOCOL = PROXY_QUALIFIER + "/protocol";
const NETWORK = PROXY_QUALIFIER + "/network";
const SERVICE = PROXY_QUALIFIER + "/service";
const TYPE = BASE_QUALIFIER + "/type";
const TYPE_PROXY = TYPE + '=proxy';

const DEPLOYMENT = {
    group: 'apps',
    version: 'v1',
    name: 'deployments',
};

function Deployer(service_account) {
    this.service_account = service_account;
    this.service_watcher = kubernetes.watch('services');
    this.service_watcher.on('updated', this.services_updated.bind(this));
    this.deployment_watcher = kubernetes.watch(DEPLOYMENT, undefined, TYPE_PROXY);
    this.deployment_watcher.on('updated', this.deployments_updated.bind(this));
}

function has_network_annotation (service) {
    return service.metadata.annotations && service.metadata.annotations[NETWORK];
}

Deployer.prototype.services_updated = function (services) {
    log.info('services updated');
    //any services with network set but deployed not set => deploy
    services.filter(has_network_annotation).forEach(this.verify_deployment.bind(this));
};

Deployer.prototype.deployments_updated = function (deployments) {
    log.info('proxy deployments updated');
    //ensure proxy deployments still match service definitions
    deployments.forEach(this.verify_matches_service.bind(this));
};

Deployer.prototype.verify_deployment = function (service) {
    var self = this;
    log.info('Verifying proxy deployment for %s', service.metadata.name);
    //ensure a proxy is deployed for the service and the service is
    //correctly configured for it
    var deployment_name = service.metadata.name + '-icproxy';
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

    var network = service.metadata.annotations[NETWORK];
    if (network === undefined) {
        this.undeploy(service, deployment);
    } else {
        var update = false;
        update = !this.verify_network(network, deployment);
        update = !this.verify_proxy_config(service, deployment);

        if (update) {
            log.info('Updating proxy deployment %s', deployment.metadata.name);
            kubernetes.put(DEPLOYMENT, deployment).then(function () {
                log.info('Updated proxy deployment %s', deployment.metadata.name);
            }).catch(function (code, error) {
                log.error('Failed to update proxy deployment for %s: %s %s', deployment.metadata.name, code, error);
            });
        }
    }

    //check selector on service is as expected
    if (!this.verify_proxy_selector(service, deployment)) {
        log.info('Updating service selector for %s', service.metadata.name);
        kubernetes.put('services', service).then(function () {
            log.info('Updated service selector for %s', service.metadata.name);
        }).catch(function (code, error) {
            log.error('Failed to update service selector for %s: %s %s', service.metadata.name, code, error);
        });
    }

}

Deployer.prototype.verify_matches_service = function (deployment) {
    var self = this;
    var service_name = deployment.metadata.annotations[SERVICE];
    if (service_name === undefined) {
        log.error('Annotation %s not found on deployment %s', SERVICE, deployment.metadata.name);
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

Deployer.prototype.verify_proxy_selector = function (service, deployment) {
    if (equivalent_selector(service.spec.selector, deployment.spec.selector.matchLabels)) {
        return true;
    } else {
        service.spec.selector = deployment.spec.selector.matchLabels;
        return false;
    }
};

function equivalent_proxy_config (a, b) {
    return a.sort().join(',') === b.sort().join(',');
}

function get_proxy_config (service) {
    //TODO: support different protocols on different ports
    //TODO: is http the right default? tcp is more conservative, but likely less commonly what is wanted
    var protocol = service.metadata.annotations[PROTOCOL] || 'http';
    var address = service.metadata.annotations[ADDRESS] || service.metadata.name;
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
        return false;
    }
};

Deployer.prototype.verify_network = function (network, deployment) {
    function match_network (volume) {
        return volume.secret === network;
    }
    //does the deployment have a connect.json mounted in for the
    //correct network?
    if (deployment.spec.template.spec.volumes && deployment.spec.template.spec.volumes.connect) {
        if (deployment.spec.template.spec.volumes.connect.secret === network) {
            return true;
        } else {
            deployment.spec.template.spec.volumes.connect.secret = network;
            return false;
        }
    } else {
        //TODO: need to mount in any credentials required for connect as well
        deployment.spec.template.spec.volumes = [{name: 'connect', secret: { secretName: network }}];
        // now also need to verify the volumeMounts in deployment.spec.template.spec.containers.proxy
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
        return original;
    }).then(function (result) {
        var code = result.code;
        if (code >=200 && code < 300) {
            log.info('Restored selector for %s', service.metadata.name);
            log.info('Deleting deployment %s', deployment.metadata.name);
            kubernetes.delete_(DEPLOYMENT, deployment.metadata.name).then(function (code, description) {
                if (code >=200 && code < 300) {
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

Deployer.prototype.deploy = function (service) {
    var proxy_selector = {};
    proxy_selector[SERVICE] = service.metadata.name;
    var original_selector = service.spec.selector;
    // deploy the proxy
    var deployment = {
        metadata: {
            name: service.metadata.name + '-icproxy', //TODO centralise this naming convention
            annotations : {},
        },
        spec: {
            selector: {
                matchLabels: proxy_selector,
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
                                value: stringify_selector(original_selector)
                            }
                        ],
                        image: 'quay.io/skupper/icproxy',
                        imagePullPolicy: 'IfNotPresent',
                        volumeMounts: [{
                            name: 'connect',
                            mountPath: "/etc/messaging/"
                        }],
                    }],
                    volumes: [{
                        name: 'connect',
                        secret: {
                            secretName: service.metadata.annotations[NETWORK]
                        }
                    }]
                }
            }
        }
    };
    deployment.metadata.annotations[SERVICE] = service.metadata.name;
    deployment.spec.template.metadata.labels[SERVICE] = service.metadata.name;
    log.info('Deploying proxy for %s', service.metadata.name);
    kubernetes.post(DEPLOYMENT, deployment).then(function (code, description) {
        if (code >=200 && code < 300) {
            log.info('Deployed proxy for %s', service.metadata.name);
            // change the selector
            log.info('Updating selector for %s', service.metadata.name);
            kubernetes.update('services', service.metadata.name, function (original) {
                original.spec.selector = proxy_selector;
                return original;
            }).then(function (result) {
                var code = result.code;
                if (code >=200 && code < 300) {
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
    });

};

var deployer = new Deployer(process.env.ICPROXY_SERVICE_ACCOUNT || 'icproxy');
