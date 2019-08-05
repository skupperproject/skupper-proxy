### directed mode

The deployer can be run in conjunction with an agent that will
automatically deploy a local router and connect it to a network by
receiving commands from a central director.

To use this, the central director needs to be started on a public
cluster somewhere and the desired network needs to defined, including
identifying all routers. The network definition uses the format from
https://github.com/skubaproject/skoot/blob/master/network-configs/network1.conf
(though without the Console element and the urls) and should be stored
in a conifgmap, e.g.

```
oc create configmap icproxy-network --from-file=network=./test-network.conf
```

TODO: At present the director does not check for updates to the
network definition, that is soon to be added though. The network
definition must therefore at present be in place before the director
is started.

To start the director:

```
oc apply -f examples/directed/director_with_route.yaml
```

This will result in configmaps being created for each identified
router in the network with the name <router-id>-bootstrap. These can
be exported and applied in the respective clusters. Doing so will
start up the deployer/agent with an appropriate certificate, such that
it connects to the director. The director will then tell direct it to
set up the router correctly. Services can then be annotated as when
using the deployer on its own.
