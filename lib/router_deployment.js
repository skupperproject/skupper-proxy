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

var to_yaml = require('js-yaml').dump;
var rc = require('./router_config.js');
var myutils = require('./utils.js');

function deployment (name, containers) {
    return {
        apiVersion: 'apps/v1',
        kind: 'Deployment',
        metadata: {
            name: name
        },
        spec: {
            selector: {
                matchLabels: {
                    deployment: name
                }
            },
            replicas: 1,
            template: {
                metadata: {
                    labels: {
                        deployment: name
                    }
                },
                spec: {
                    containers: containers || [],
                    volumes: []
                }
            }
        }
    };
};


function client_ports() {
    return [
        {
            name: 'amqps',
            port: 5671
        },
        {
            name: 'http',
            port: 8080
        }
    ];
}

function inter_router_ports() {
    return [
        {
            name: 'inter-router',
            port: 55671,
        },
        {
            name: 'edge',
            port: 45671,
        }
    ];
}

function to_container_port(p) {
    return {
        name: p.name,
        containerPort: p.port
    };
}

function edge_router_ports() {
    return client_ports().map(to_container_port);
}

function interior_router_ports() {
    return client_ports().concat(inter_router_ports()).map(to_container_port);
}

function inter_router_secrets_name(deployment_name) {
    return deployment_name + '-inter-router-certs';
}

function router_container(name, config, ports) {
    return {
        name: 'router',
        image: 'quay.io/interconnectedcloud/qdrouterd',
        imagePullPolicy: 'IfNotPresent',
        ports: ports,
        env: [
            {
                name: 'APPLICATION_NAME',
                value: name
            },
            {
                name: 'QDROUTERD_CONF',
                value: config
            },
            {
                name: 'POD_NAMESPACE',
                valueFrom: {
                    fieldRef: {
                        apiVersion: 'v1',
                        fieldPath: 'metadata.namespace'
                    }
                }
            },
            {
                name: 'POD_IP',
                valueFrom: {
                    fieldRef: {
                        apiVersion: 'v1',
                        fieldPath: 'status.podIP'
                    }
                }
            }
        ],
        volumeMounts: []
    };
}

function router_config (mode) {
    var config = rc.create(mode, '${HOSTNAME}');
    config.listener({port: 5672, host: '127.0.0.1', role: 'normal'});
    config.listener({port: 5671, role: 'normal', sslProfile: 'amqps', saslMechanisms: 'EXTERNAL', authenticatePeer: true});
    config.listener({port: 8080, role: 'normal', http: true});
    config.add_ssl_profile_for_mutual_auth('inter-router');
    config.add_ssl_profile_for_mutual_auth('amqps');
    config.address({prefix: 'events', distribution: 'multicast'});
    return config;
}

function edge_router_config (connectors) {
    var config = router_config('edge');
    connectors.forEach(function (connector) {
        config.connector(myutils.merge({role:'edge', sslProfile:'inter-router'}, connector));
    });
    return config.toString();
}

function interior_router_config (connectors) {
    var config = router_config('interior');
    config.listener({port: 55671, role: 'inter-router', sslProfile: 'inter-router', saslMechanisms: 'EXTERNAL', authenticatePeer: true});
    config.listener({port: 45671, role: 'edge', sslProfile: 'inter-router', saslMechanisms: 'EXTERNAL', authenticatePeer: true});
    connectors.forEach(function (connector) {
        config.connector(myutils.merge({role:'inter-router', sslProfile:'inter-router'}, connector));
    });
    return config.toString();
}

function add_container (deployment, container) {
    deployment.spec.template.spec.containers.push(container);
}

function mount_config_volume (deployment, container, name, path) {
    container.volumeMounts.push({name: name, mountPath: path});
    deployment.spec.template.spec.volumes.push({name: name, configMap: {name: name}});
    return deployment;
}

function mount_secret_volume (deployment, container, name, path) {
    container.volumeMounts.push({name: name, mountPath: path});
    deployment.spec.template.spec.volumes.push({name: name, secret: {secretName: name}});
    return deployment;
}

function router_deployment (name, container) {
    var d = mount_secret_volume(deployment(name, [container]), container, inter_router_secrets_name(name), rc.CERTS_PATH + '/inter-router/');
    mount_secret_volume(d, container, name, rc.CERTS_PATH + '/amqps/');
    return d;
}

module.exports.edge_router_deployment = function (name, connectors) {
    return router_deployment(name, router_container(name, edge_router_config(connectors || []), edge_router_ports()));
};

module.exports.interior_router_deployment =function (name, connectors) {
    return router_deployment(name, router_container(name, interior_router_config(connectors || []), interior_router_ports()));
};

function name_finder(name) {
    return function (item) {
        return item.name === name;
    };
}

function find_by_name(items, name) {
    for (var i = 0; i < items.length; i++) {
        if (items[i].name === name) return items[i];
    }
    return undefined;
}

module.exports.get_router_config = function (deployment) {
    var container = find_by_name(deployment.spec.template.spec.containers, 'router');
    return find_by_name(container.env, 'QDROUTERD_CONF').value;
};

module.exports.set_router_config = function (deployment, config) {
    var container = find_by_name(deployment.spec.template.spec.containers, 'router');
    find_by_name(container.env, 'QDROUTERD_CONF').value = config;
};

function service(name, deployment, ports) {
    return {
        apiVersion: 'v1',
        kind: 'Service',
        metadata: {
            name: name
        },
        spec: {
            ports: ports,
            selector: {
                deployment: deployment
            }
        }
    };
}

module.exports.client_service = function (name) {
    return service(name, name, client_ports());
};

module.exports.inter_router_service = function (name, type) {
    var svc = service(name + '-inter-router', name, inter_router_ports());
    if (type) svc.spec.type = type;
    return svc;
};

module.exports.inter_router_secret = function (name, certs) {
    return {
        apiVersion: 'v1',
        kind: 'Secret',
        metadata: {
            name: inter_router_secrets_name(name)
        },
        type: 'kubernetes.io/tls',
        data: certs
    };
};

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

function equivalent_selectors(a, b) {
    for (var k in a) {
        if (a[k] !== b[k]) return false;
    }
    for (var k in b) {
        if (a[k] !== b[k]) return false;
    }
    return true;
}

module.exports.equivalent_services = function (a, b) {
    return equivalent_selectors(a.spec.selector, b.spec.selector) && equivalent_ports(a.spec.ports, b.spec.ports);
};

function configmap(name, data) {
    return {
        apiVersion: 'v1',
        kind: 'ConfigMap',
        metadata: {
            name: name
        },
        data: data
    };
}

function deployer_agent_container(name) {
    return {
        name: 'agent',
        image: process.env.DEPLOYER_IMAGE || 'quay.io/gordons/icproxy-deployer',
        imagePullPolicy: 'Always',
        env: [
            {
                name: 'AGENT_NAME',
                value: name
            },
            {
                name: 'ROUTER_DEPLOY',
                value: 'directed'
            }
        ],
        volumeMounts: []
    };
}

function service_account(name) {
    return {
        apiVersion: 'v1',
        kind: 'ServiceAccount',
        metadata: {
            name: name
        }
    };
}

function role_binding(serviceaccount, role) {
    return {
        apiVersion: 'v1',
        kind: 'RoleBinding',
        metadata: {
            name: serviceaccount + '-' + role
        },
        subjects: [
            {
                kind: 'ServiceAccount',
                name: serviceaccount
            }
        ],
        roleRef: {
            kind: 'Role',
            name: role
        }
    };
}

module.exports.bootstrap_config = function (configname, secret) {
    var container = deployer_agent_container(secret.metadata.name);
    var d = deployment(secret.metadata.name, [container]);
    mount_secret_volume(d, container, secret.metadata.name, '/etc/messaging');
    var sa = 'icproxy-agent';
    d.spec.template.spec.serviceAccountName = sa;

    var yaml = to_yaml(secret);
    [d, service_account(sa), role_binding(sa, 'edit'), service_account('icproxy'), role_binding('icproxy', 'view')].forEach(function (o) {
        yaml += '\n---\n';
        yaml += to_yaml(o);
    });

    return configmap(configname, {'bootstrap.yaml': yaml});
};
