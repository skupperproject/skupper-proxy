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


module.exports.set_owner_references = function (object) {
    if (owner_references) {
        object.metadata.ownerReferences = owner_references;
    }
}
