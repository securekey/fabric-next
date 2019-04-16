# Fabric Next

Fabric Next is a custom build of Hyperledger Fabric with the following enhancements:
- Cherry-picks of certain features not scheduled to be included in Fabric 1.1
- Experimental build tag enabled
- Dynamic build to allow loading plugins

##### Build

Prerequisites:
- A Git Client
- Docker - 17.06.2-ce or later
- Docker Compose - 1.14.0 or later
- You may need libtool - sudo apt-get install -y libtool (linux) or brew install libtool (macOS)
- You may need GNU tar on macOS -  brew install gnu-tar (you should explicitly add the binary to the path, 
  as instructed when installing to make sure it is preferred over the library that is shipped with macOS)

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
- GO_TAGS=pluginsenabled : Enable loading system chaincodes as plugins.
