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

var net = require('net');

var hostname = Buffer.from(process.env.HOSTNAME + ': ');

function accepted(socket) {
    console.log('connection accepted');
    socket.on('data', function (data) {
        console.log('read %s bytes', data.length);
        socket.write(hostname);
        socket.write(data);
    });
    socket.on('end', function (data) {
        if (data) {
            socket.write(hostname);
            socket.write(data);
        }
        socket.end();
        console.log('connection closed');
    });
}

var server = net.createServer(accepted);
server.on('error', function (err) {
    console.error(err);
});
server.listen(process.env.PORT || 9090, function () {
    console.log('listening on %s', server.address().port);
});
