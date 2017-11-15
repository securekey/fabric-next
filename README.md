# Fabric Next

Fabric Next is a custom build of Hyperledger Fabric with the following enhancements:
- Cherry-picks of certain features not scheduled to be included in Fabric 1.1
- Experimental build tag enabled
- Dynamic build to allow loading plugins

##### Build
Note: We assume a working Golang(v1.9.2) and Docker(v17.09.0-ce) setup.

*Note:* The tagged version of fabric-next being used must match the corresponding tag in fabric-snaps. e.g v17.11.1 of fabric-next is compatible with v17.11.1 of fabric-snaps.

To build fabric next run:
```
$ cd $GOPATH/src/github.com/securekey/fabric-next/scripts
$ ./pull_fabric.sh
```
This script will cherry-pick fabric next and build it.

'Make' flags used:
- DOCKER_DYNAMIC_LINK=true : Dynamic builds are required to load Go plugins into fabric.
- BASE_DOCKER_NS=securekey : These custom base images include Go 1.9.2 and the C libraries required by fabric for dynamic linking (currently libtool). The Dockerfiles that produce these images can be found in the `scripts/images` directory. Images are also produced by the script.
(the two flags above may be omitted if you already have a dynamically compiled fabric peer)
