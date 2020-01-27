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
var service_utils = require('./service_utils.js');

function Scaling () {
    this.started = false;
};

Scaling.prototype.start = function () {
    this.statefulset_watcher = kubernetes.watch(kubernetes.STATEFULSET);
    this.statefulset_watcher.on('updated', this.statefulsets_updated.bind(this));
};

Scaling.prototype.definitions_updated = function (definitions) {
    this.headless = {};
    var have_headless = false;
    for (var name in definitions) {
        var service = definitions[name];
        if (service.headless !== undefined && service.origin === undefined) {
            this.headless[service.headless.name] = service;
            have_headless = true;
        }
    }
    if (!this.started && have_headless) {
        this.started = true;
        this.start();
    }
};

Scaling.prototype.statefulsets_updated = function (statefulsets) {
    var changed = [];
    for (var i in statefulsets) {
        var s = statefulsets[i];
        var def = this.headless[s.metadata.name];
        if (def && def.headless.size != s.spec.replicas) {
            log.info('scaling headless service %s from %s to %s', s.metadata.name, def.headless.size, s.spec.replicas);
            def.headless.size = s.spec.replicas;
            changed.push(def);
        }
    }
    service_utils.update_skupper_services(changed, [], 'scaled');
};

module.exports = function () {
    return new Scaling();
};
