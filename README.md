== icproxy: a tcp/http proxy prototype using apache qpid dispatch router ==

The proxy is controlled via two environment variables:

ICPROXY_CONFIG through which the proxying is controlled

ICPROXY_POD_SELECTOR through which any backend processes to be proxied
to are identified

The config string is a comma separate list of bridges, Each bridge has
a source and target separated by '=>'. A source or target is a protocol (http, tcp or
amqp) and a port (for http and tcp) or address (for amqp), separated
by a colon.

E.g. tcp:9090=>amqp:foo implies listening on 9090 for tcp data which
is then sent over a link to foo

== examples ==

Under examples there are a couple of dummy services for trying things out.

First set up the router:

```
oc apply -f examples/router.yaml
```

Then deploy one (or both) of the example services:

```
oc apply -f examples/http/deployment.yaml
```
and/or
```
oc apply -f examples/tcp/deployment.yaml
```

Then create the service proxy for the service(s):

```
oc apply -f examples/http/service-proxy.yaml
```
and/or
```
oc apply -f examples/tcp/service-proxy.yaml
```

You can then try:

```
curl $(oc get service myservice -o=jsonpath='{.spec.clusterIP}'):8080/foo -H host:myservice
```
and/or
```
telnet $(oc get service echo -o=jsonpath='{.spec.clusterIP}') 9090
```
