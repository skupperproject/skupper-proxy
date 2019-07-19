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

var utils = require('./utils.js');

const DEFAULTS = {
    listener: {
        host: '0.0.0.0'
    }
};
const TYPES = ['router', 'listener', 'connector', 'sslProfile', 'address'];
const SSL_PROFILE_FILE_NAMES = [{field:'privateKeyFile', file:'tls.key'}, {field:'certFile', file:'tls.crt'}, {field:'caCertFile', file:'ca.crt'}];
const CERTS_PATH = '/etc/qpid-dispatch-certs/';

function RouterConfig(mode, id) {
    this.elements = [];
    this.router({mode: mode || 'interior', id: id || '${HOSTNAME}'});
}

RouterConfig.prototype.add_element = function (type, fields) {
    this.elements.push({type:type, fields:utils.merge(fields, DEFAULTS[type])});
};

TYPES.forEach(function (type) {
    RouterConfig.prototype[type] = function (fields) {
        this.add_element(type, fields);
    };
});

RouterConfig.prototype.add_ssl_profile_for_mutual_auth = function (name, path) {
    var base = path || (CERTS_PATH + name);
    var profile = {name: name};
    SSL_PROFILE_FILE_NAMES.forEach(function (f) {
        profile[f.field] = base + '/' + f.file;
    });
    this.sslProfile(profile);
};

function element_to_string(element) {
    var parts = [];
    parts.push(element.type + ' {');
    for (var k in element.fields) {
        if (element.fields[k] !== undefined) {
            parts.push('  ' + k + ' : ' + element.fields[k]);
        }
    }
    parts.push('}');
    return parts.join('\n');
}

RouterConfig.prototype.toString = function () {
    var s = '';
    return this.elements.map(element_to_string).join('\n\n');
    return s;
};

module.exports.create = function (mode, id) {
    return new RouterConfig(mode, id);
};
module.exports.CERTS_PATH = CERTS_PATH;
