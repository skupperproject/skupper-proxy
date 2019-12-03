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

const BASE_QUALIFIER = "skupper.io";
const INTERNAL_QUALIFIER = "internal." + BASE_QUALIFIER;

const ADDRESS = BASE_QUALIFIER + "/address";
const PROXY = BASE_QUALIFIER + "/proxy";
const VERSION = BASE_QUALIFIER + "/version";
const WEIGHT = BASE_QUALIFIER + "/weight";

const CONTROLLED = INTERNAL_QUALIFIER + "/controlled";
const SERVICE = INTERNAL_QUALIFIER + "/service";
const ORIGIN = INTERNAL_QUALIFIER + "/origin";
const ORIGINAL_SELECTOR = INTERNAL_QUALIFIER + "/original-selector";
const TYPE = INTERNAL_QUALIFIER + "/type";

const TYPE_PROXY = TYPE + '=proxy';

module.exports = {
    ADDRESS: ADDRESS,
    CONTROLLED: CONTROLLED,
    PROXY: PROXY,
    VERSION: VERSION,
    WEIGHT: WEIGHT,
    SERVICE: SERVICE,
    ORIGIN: ORIGIN,
    ORIGINAL_SELECTOR: ORIGINAL_SELECTOR,
    TYPE: TYPE,
    TYPE_PROXY: TYPE_PROXY
};
