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
var log = require('./log.js').logger();
var owner_ref = require("./owner_ref.js");

module.exports.set_last_applied = function (service) {
    var subset = {
        apiVersion:"v1",
        kind:"Service",
        metadata: {
            annotations: service.metadata.annotations || {},
            name: service.metadata.name
        },
        spec:{
            ports: service.spec.ports,
            selector: service.spec.selector
        }
    };
    delete subset.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"];
    service.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"] = JSON.stringify(subset);
};

function is_success_code(code) {
    return code >= 200 && code < 300;
}

module.exports.update_skupper_services = function (changed, deleted, origin) {
    if (changed.length || deleted.length) {
        kubernetes.update('configmaps', 'skupper-services', function (original) {
            var result;
            if (original) {
                result = original;
                if (result.data === undefined) {
                    result.data = {};
                }
            } else {
                result = {
                    apiVersion: 'v1',
                    kind: 'ConfigMap',
                    metadata: {
                        name: 'skupper-services'
                    },
                    data: {}
                };
                owner_ref.set_owner_references(result);
            }
            changed.forEach(function (service) {
                result.data[service.address] = JSON.stringify(service);
            });
            deleted.forEach(function (name) {
                delete result.data[name];
            });
            log.info('Updating skupper-services to %j', result);
            return result;
        }).then(function (result) {
            if (is_success_code(result.code)) {
                log.info('updated services from %s', origin);
            } else {
                log.error('failed to update services from %s: %s', origin, result.code);
            }
        }).catch(function (error) {
                log.error('failed to update services from %s: %s', origin, error);
        });
    }
};
