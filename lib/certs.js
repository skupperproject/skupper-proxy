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

var child_process = require('child_process');
var fs = require('fs');
var path = require('path');
var util = require('util');
var kubernetes = require('./kubernetes.js').client();
var log = require("./log.js").logger();
var myutils = require('./utils.js');

const accessFile = util.promisify(fs.access);
const mkdir = util.promisify(fs.mkdir);
const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);

function get_tls_secret_path(issuer, name) {
    return path.join(process.env.BASEDIR || '/tmp', issuer, name, 'secret.json');
}

function get_ca_dir(issuer) {
    return path.join(process.env.BASEDIR || '/tmp', issuer, 'ca');
}

function get_ca_file_path(issuer, filename) {
    return path.join(process.env.BASEDIR || '/tmp', issuer, 'ca', filename);
}

function get_ca_secret_path(issuer) {
    return get_ca_file_path(issuer, 'secret.json');
}

function save_secret(secret) {
    return kubernetes.post('secrets', secret);
}

function generate_secret(args, secret_json, env) {
    var cmd = path.resolve(__dirname, '../bin/generate_cert');
    var p = child_process.spawn(cmd, args, {env: myutils.merge(process.env, env || {})});
    p.stdout.on('data', function (data) {
        log.debug('stdout[generate_cert]: %s', data);
    });
    p.stderr.on('data', function (data) {
        log.debug('stderr[generate_cert]: %s', data);
    });
    return new Promise(function (resolve, reject) {
        p.on('exit', function (code, signal) {
            if (code) {
                log.error('%s %s exited with %s', cmd, args, code);
                reject('Could not generate secret: ' + cmd + ' exited with ' + code);
            } else {
                log.info('%s %s completed, retrieving %s', cmd, args, secret_json);
                return readFile(secret_json).then(function (data) {
                    try{
                        resolve(JSON.parse(data));
                    } catch(e) {
                        reject(e);
                    }
                }).catch(reject);
            }
        });
    });
}

function generate_and_save_secret(args, secret_json) {
    return generate_secret(args, secret_json).then(function (secret) {
        return save_secret(secret);
    });
}

function write_ca_file(issuer, secret, filename) {
    return writeFile(get_ca_file_path(issuer, filename), myutils.base64decode(secret.data[filename]));
}

function write_key(issuer, secret) {
    return write_ca_file(issuer, secret, 'tls.key');
}

function write_crt(issuer, secret) {
    return write_ca_file(issuer, secret, 'tls.crt');
}

function write_ca(issuer, secret) {
    return mkdir(get_ca_dir(issuer), {recursive: true}).then(function () {
        return Promise.all([write_key(issuer, secret), write_crt(issuer, secret)]);
    });
}

function extract_ca(issuer) {
    return kubernetes.get('secrets', issuer).then(function (secret) {
        return write_ca(issuer, secret);
    });
}

function ensure_ca(issuer) {
    return accessFile(get_ca_file_path(issuer, 'tls.key')).catch(function () {
        log.info('ca key does not exist, checking for secret...');
        //file does not exist locally
        return extract_ca(issuer).catch(function (code) {
            if (code === 404) {
                log.info('ca secret does not exist, generating it...');
                //secret does not yet exist
                return generate_and_save_secret([issuer], get_ca_secret_path(issuer));
            } else {
                log.error('Failed to extract CA: %s', code);
                return Promise.reject('Failed to extract CA: ' + code);
            }
        });
    });
}

function Issuer(name, storeCA) {
    this.name = name;
    this.storeCA = storeCA;
    this.initialised = false;
}

Issuer.prototype.init = function () {
    if (!this.initialised && this.storeCA) {
        this.initialised = true;
        return ensure_ca(this.name);
    } else {
        return Promise.resolve();
    }
}

Issuer.prototype.create_tls_secret = function (name, alternate_names, connect_info) {
    return this.generate_tls_secret(name, alternate_names, connect_info).then(function (secret) {
        return save_secret(secret);
    });
};

Issuer.prototype.generate_tls_secret = function (name, alternate_names, connect_info) {
    var args = [this.name, name].concat(alternate_names || []);
    var env = {};
    if (connect_info) {
        env.CONNECT_HOST = connect_info.host;
        if (connect_info.port) env.CONNECT_PORT = connect_info.port;
    }
    var path = get_tls_secret_path(this.name, name);
    return this.init().then(function () {
        return generate_secret(args, path, env);
    });
};

module.exports.issuer = function (name) {
    return new Issuer(name, true);
};

module.exports.tls_secret = function (name, alternate_names) {
    return new Issuer(name + '-ca', false).create_tls_secret(name, alternate_names);
};

module.exports.generate_tls_secret = function (name, alternate_names) {
    return new Issuer(name + '-ca', false).generate_tls_secret(name, alternate_names);
};
