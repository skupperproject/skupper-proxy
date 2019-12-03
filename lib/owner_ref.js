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

var owner_references;

if (process.env.OWNER_NAME && process.env.OWNER_UID) {
    owner_references = [{
        apiVersion: 'apps/v1',
        kind: 'Deployment',
        name: process.env.OWNER_NAME,
        uid: process.env.OWNER_UID
    }];
}

function equivalent_owner_reference(a, b) {
    return a.apiVersion === b.apiVersion && a.kind === b.kind && a.name === b.name && a.uid === b.uid;
}

function has_equivalent_owner_reference(list, target) {
    for (var i in list) {
        if (equivalent_owner_reference(list[i], target)) {
            return true;
        }
    }
    return false;
}

function equivalent_owner_references(a, b) {
    if (a === undefined) {
        return b === undefined;
    } else if (a.length === b.length) {
        for (var i in a) {
            if (!has_equivalent_owner_reference(b, a[i])) {
                return false;
            }
        }
        return true;
    } else {
        return false;
    }
}

module.exports.set_owner_references = function (object) {
    if (owner_references) {
        object.metadata.ownerReferences = owner_references;
    }
}

module.exports.is_owner = function (object) {
    if (owner_references && object.metadata) {
        return equivalent_owner_references(object.metadata.ownerReferences, owner_references);
    }
}
