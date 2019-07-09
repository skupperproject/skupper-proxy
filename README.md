## icproxy: a tcp/http proxy prototype using apache qpid dispatch router

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

### manual setup examples

Under examples there are a couple of dummy services for trying things out.

First set up the router:

```
oc apply -f examples/router.yaml
```

(If using a router provisioned in some other way, you will need to
create a secret to tell the proxy how to connect to that router. You
can edit the examples/connect-info.yaml secret and apply that).

Then deploy one (or both) of the example services:

```
oc apply -f examples/http/deployment.yaml
```
and/or
```
oc apply -f examples/tcp/deployment.yaml
```

Then create a service account under which to run the proxy, which
needs view permission to determine the endpoints:

```
oc apply -f examples/serviceaccount.yaml
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

### deployer

There is also a deployer component that will automatically deploy the
proxy and update the service based on the presence of certain
annotations on that service.

Setup router as before, then:

```
oc apply -f examples/deployer/deployer.yaml
```

```
oc apply -f examples/deployer/http/service-proxy.yaml
oc annotate service myservice proxy.grs.github.com/network=myrouter
```

and/or

```
oc apply -f examples/deployer/tcp/service-proxy.yaml
oc annotate service echo proxy.grs.github.com/network=myrouter proxy.grs.github.com/protocol=tcp
```

Note that the network name needs to correspond to the name of a secret
containing the connect.json details.

You can then test as before:

```
curl $(oc get service myservice -o=jsonpath='{.spec.clusterIP}'):8080/foo -H host:myservice
```
and/or
```
telnet $(oc get service echo -o=jsonpath='{.spec.clusterIP}') 9090
```

(The annotations could of course have been diretcly written in to the
yaml in the first place. The above shows how an existing application
could be modified to route traffic through the router).

TODO: the removal of the network annotation should cause the service
tobe restored to its original state and the proxy deployment to be
deleted. That doesn't work yet.