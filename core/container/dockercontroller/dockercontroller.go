/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dockercontroller

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"bufio"

	"regexp"

	"github.com/fsouza/go-dockerclient"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	container "github.com/hyperledger/fabric/core/container/api"
	"github.com/hyperledger/fabric/core/container/ccintf"
	cutil "github.com/hyperledger/fabric/core/container/util"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"golang.org/x/net/context"

	pb "github.com/hyperledger/fabric/protos/peer"
)

var (
	dockerLogger             = flogging.MustGetLogger("dockercontroller")
	hostConfig               *docker.HostConfig
	extSccDockerTypeRegistry = make(map[string]*dockerSysExtContainer)
	extSccDockerInstRegistry = make(map[string]*dockerSysExtContainer)
)

type dockerSysExtContainer struct {
	chaincode         shim.Chaincode
	running           bool
	args              []string
	env               []string
	chaincodeSpecType pb.ChaincodeSpec_Type
	configPath        string
}

// getClient returns an instance that implements dockerClient interface
type getClient func() (dockerClient, error)

//DockerVM is a vm. It is identified by an image id
type DockerVM struct {
	id           string
	getClientFnc getClient
}

// dockerClient represents a docker client
type dockerClient interface {
	// CreateContainer creates a docker container, returns an error in case of failure
	CreateContainer(opts docker.CreateContainerOptions) (*docker.Container, error)
	// StartContainer starts a docker container, returns an error in case of failure
	StartContainer(id string, cfg *docker.HostConfig) error
	// AttachToContainer attaches to a docker container, returns an error in case of
	// failure
	AttachToContainer(opts docker.AttachToContainerOptions) error
	// BuildImage builds an image from a tarball's url or a Dockerfile in the input
	// stream, returns an error in case of failure
	BuildImage(opts docker.BuildImageOptions) error
	// RemoveImageExtended removes a docker image by its name or ID, returns an
	// error in case of failure
	RemoveImageExtended(id string, opts docker.RemoveImageOptions) error
	// StopContainer stops a docker container, killing it after the given timeout
	// (in seconds). Returns an error in case of failure
	StopContainer(id string, timeout uint) error
	// KillContainer sends a signal to a docker container, returns an error in
	// case of failure
	KillContainer(opts docker.KillContainerOptions) error
	// RemoveContainer removes a docker container, returns an error in case of failure
	RemoveContainer(opts docker.RemoveContainerOptions) error
}

// errors

//DockerExtSysCCRegisteredErr registered error
type DockerExtSysCCRegisteredErr string

func (s DockerExtSysCCRegisteredErr) Error() string {
	return fmt.Sprintf("%s already registered - Docker SCC", string(s))
}

//Register registers remote system chaincode for Docker with given path. The deploy should be called to initialize
func Register(path string, cc shim.Chaincode, cctype pb.ChaincodeSpec_Type, configPath string) error {
	tmp := extSccDockerTypeRegistry[path]
	if tmp != nil {
		return DockerExtSysCCRegisteredErr(path)
	}
	vm := *NewDockerVM()

	dockerLogger.Debugf("**** Registering External SCC in a docker %s, %#v, ", path, cc, cctype, configPath, vm)
	extSccDockerTypeRegistry[path] = &dockerSysExtContainer{
		chaincode:         cc,
		chaincodeSpecType: cctype,
		configPath:        configPath,
	}
	return nil
}

// NewDockerVM returns a new DockerVM instance
func NewDockerVM() *DockerVM {
	vm := DockerVM{}
	vm.getClientFnc = getDockerClient
	return &vm
}

func getDockerClient() (dockerClient, error) {
	return cutil.NewDockerClient()
}

func getDockerHostConfig() *docker.HostConfig {
	if hostConfig != nil {
		return hostConfig
	}
	dockerKey := func(key string) string {
		return "vm.docker.hostConfig." + key
	}
	getInt64 := func(key string) int64 {
		defer func() {
			if err := recover(); err != nil {
				dockerLogger.Warningf("load vm.docker.hostConfig.%s failed, error: %v", key, err)
			}
		}()
		n := viper.GetInt(dockerKey(key))
		return int64(n)
	}

	var logConfig docker.LogConfig
	err := viper.UnmarshalKey(dockerKey("LogConfig"), &logConfig)
	if err != nil {
		dockerLogger.Warningf("load docker HostConfig.LogConfig failed, error: %s", err.Error())
	}
	networkMode := viper.GetString(dockerKey("NetworkMode"))
	if networkMode == "" {
		networkMode = "host"
	}
	dockerLogger.Debugf("docker container hostconfig NetworkMode: %s", networkMode)

	hostConfig = &docker.HostConfig{
		CapAdd:  viper.GetStringSlice(dockerKey("CapAdd")),
		CapDrop: viper.GetStringSlice(dockerKey("CapDrop")),

		DNS:         viper.GetStringSlice(dockerKey("Dns")),
		DNSSearch:   viper.GetStringSlice(dockerKey("DnsSearch")),
		ExtraHosts:  viper.GetStringSlice(dockerKey("ExtraHosts")),
		NetworkMode: networkMode,
		IpcMode:     viper.GetString(dockerKey("IpcMode")),
		PidMode:     viper.GetString(dockerKey("PidMode")),
		UTSMode:     viper.GetString(dockerKey("UTSMode")),
		LogConfig:   logConfig,

		ReadonlyRootfs:   viper.GetBool(dockerKey("ReadonlyRootfs")),
		SecurityOpt:      viper.GetStringSlice(dockerKey("SecurityOpt")),
		CgroupParent:     viper.GetString(dockerKey("CgroupParent")),
		Memory:           getInt64("Memory"),
		MemorySwap:       getInt64("MemorySwap"),
		MemorySwappiness: getInt64("MemorySwappiness"),
		OOMKillDisable:   viper.GetBool(dockerKey("OomKillDisable")),
		CPUShares:        getInt64("CpuShares"),
		CPUSet:           viper.GetString(dockerKey("Cpuset")),
		CPUSetCPUs:       viper.GetString(dockerKey("CpusetCPUs")),
		CPUSetMEMs:       viper.GetString(dockerKey("CpusetMEMs")),
		CPUQuota:         getInt64("CpuQuota"),
		CPUPeriod:        getInt64("CpuPeriod"),
		BlkioWeight:      getInt64("BlkioWeight"),
	}

	return hostConfig
}

func (vm *DockerVM) createContainer(ctxt context.Context, client dockerClient,
	imageID string, containerID string, args []string,
	env []string, attachStdout bool) error {
	config := docker.Config{Cmd: args, Image: imageID, Env: env, AttachStdout: attachStdout, AttachStderr: attachStdout}
	copts := docker.CreateContainerOptions{Name: containerID, Config: &config, HostConfig: getDockerHostConfig()}
	dockerLogger.Debugf("Create container: %s", containerID)
	_, err := client.CreateContainer(copts)
	if err != nil {
		return err
	}
	dockerLogger.Debugf("Created container: %s", imageID)
	return nil
}

func (vm *DockerVM) deployImage(client dockerClient, ccid ccintf.CCID,
	args []string, env []string, reader io.Reader) error {
	id, err := vm.GetVMName(ccid)
	if err != nil {
		return err
	}
	outputbuf := bytes.NewBuffer(nil)
	opts := docker.BuildImageOptions{
		Name:         id,
		Pull:         false,
		InputStream:  reader,
		OutputStream: outputbuf,
	}

	if err := client.BuildImage(opts); err != nil {
		dockerLogger.Errorf("Error building images: %s", err)
		dockerLogger.Errorf("Image Output:\n********************\n%s\n********************", outputbuf.String())
		return err
	}

	dockerLogger.Debugf("Created image: %s", id)

	return nil
}

//Deploy use the reader containing targz to create a docker image
//for docker inputbuf is tar reader ready for use by docker.Client
//the stream from end client to peer could directly be this tar stream
//talk to docker daemon using docker Client and build the image
func (vm *DockerVM) Deploy(ctxt context.Context, ccid ccintf.CCID,
	args []string, env []string, reader io.Reader) error {

	// verify if matches External SCC first
	if ccid.IsSysSCC {
		path := ccid.ChaincodeSpec.ChaincodeId.Path

		ipctemplate := extSccDockerTypeRegistry[path]
		if ipctemplate == nil {
			return fmt.Errorf(fmt.Sprintf("%s not registered", path))
		}

		if ipctemplate.chaincode == nil {
			return fmt.Errorf(fmt.Sprintf("%s system chaincode does not contain chaincode instance", path))
		}

		instName, _ := vm.GetVMName(ccid)
		_, err := vm.getDockerExtSCCInstance(ctxt, ipctemplate, instName, args, env)

		//FUTURE ... here is where we might check code for safety
		dockerLogger.Debugf("registered : %s", path)

		return err
	}

	client, err := vm.getClientFnc()
	switch err {
	case nil:
		if err = vm.deployImage(client, ccid, args, env, reader); err != nil {
			return err
		}
	default:
		return fmt.Errorf("Error creating docker client: %s", err)
	}
	return nil
}

//Start starts a container using a previously created docker image
func (vm *DockerVM) Start(ctxt context.Context, ccid ccintf.CCID,
	args []string, env []string, builder container.BuildSpecFactory, prelaunchFunc container.PrelaunchFunc) error {
	imageID, err := vm.GetVMName(ccid)
	if err != nil {
		return err
	}

	// verify if matches External SCC first
	if ccid.IsSysSCC {
		path := ccid.ChaincodeSpec.ChaincodeId.Path

		ectemplate := extSccDockerTypeRegistry[path]

		if ectemplate == nil {
			return fmt.Errorf(fmt.Sprintf("%s not registered", path))
		}

		ec, err := vm.getDockerExtSCCInstance(ctxt, ectemplate, imageID, args, env)

		if err != nil {
			return fmt.Errorf(fmt.Sprintf("could not create instance for %s", imageID))
		}

		if ec.running {
			return fmt.Errorf(fmt.Sprintf("chaincode running %s", path))
		}
	}

	client, err := vm.getClientFnc()
	if err != nil {
		dockerLogger.Debugf("start - cannot create client %s", err)
		return err
	}

	containerID := strings.Replace(imageID, ":", "_", -1)
	attachStdout := viper.GetBool("vm.docker.attachStdout")

	//stop,force remove if necessary
	dockerLogger.Debugf("Cleanup container %s", containerID)
	vm.stopInternal(ctxt, client, containerID, 0, false, false)

	dockerLogger.Debugf("Start container %s", containerID)
	err = vm.createContainer(ctxt, client, imageID, containerID, args, env, attachStdout)
	if err != nil {
		//if image not found try to create image and retry
		if err == docker.ErrNoSuchImage {
			if builder != nil {
				dockerLogger.Debugf("start-could not find image <%s> (container id <%s>), because of <%s>..."+
					"attempt to recreate image", imageID, containerID, err)

				reader, err1 := builder()
				if err1 != nil {
					dockerLogger.Errorf("Error creating image builder for image <%s> (container id <%s>), "+
						"because of <%s>", imageID, containerID, err1)
				}

				if err1 = vm.deployImage(client, ccid, args, env, reader); err1 != nil {
					return err1
				}

				dockerLogger.Debug("start-recreated image successfully")
				if err1 = vm.createContainer(ctxt, client, imageID, containerID, args, env, attachStdout); err1 != nil {
					dockerLogger.Errorf("start-could not recreate container post recreate image: %s", err1)
					return err1
				}
			} else {
				dockerLogger.Errorf("start-could not find image <%s>, because of %s", imageID, err)
				return err
			}
		} else {
			dockerLogger.Errorf("start-could not recreate container <%s>, because of %s", containerID, err)
			return err
		}
	}

	if attachStdout {
		// Launch a few go-threads to manage output streams from the container.
		// They will be automatically destroyed when the container exits
		attached := make(chan struct{})
		r, w := io.Pipe()

		go func() {
			// AttachToContainer will fire off a message on the "attached" channel once the
			// attachment completes, and then block until the container is terminated.
			// The returned error is not used outside the scope of this function. Assign the
			// error to a local variable to prevent clobbering the function variable 'err'.
			err := client.AttachToContainer(docker.AttachToContainerOptions{
				Container:    containerID,
				OutputStream: w,
				ErrorStream:  w,
				Logs:         true,
				Stdout:       true,
				Stderr:       true,
				Stream:       true,
				Success:      attached,
			})

			// If we get here, the container has terminated.  Send a signal on the pipe
			// so that downstream may clean up appropriately
			_ = w.CloseWithError(err)
		}()

		go func() {
			// Block here until the attachment completes or we timeout
			select {
			case <-attached:
				// successful attach
			case <-time.After(10 * time.Second):
				dockerLogger.Errorf("Timeout while attaching to IO channel in container %s", containerID)
				return
			}

			// Acknowledge the attachment?  This was included in the gist I followed
			// (http://bit.ly/2jBrCtM).  Not sure it's actually needed but it doesn't
			// appear to hurt anything.
			attached <- struct{}{}

			// Establish a buffer for our IO channel so that we may do readline-style
			// ingestion of the IO, one log entry per line
			is := bufio.NewReader(r)

			// Acquire a custom logger for our chaincode, inheriting the level from the peer
			containerLogger := flogging.MustGetLogger(containerID)
			logging.SetLevel(logging.GetLevel("peer"), containerID)

			for {
				// Loop forever dumping lines of text into the containerLogger
				// until the pipe is closed
				line, err2 := is.ReadString('\n')
				if err2 != nil {
					switch err2 {
					case io.EOF:
						dockerLogger.Infof("Container %s has closed its IO channel", containerID)
					default:
						dockerLogger.Errorf("Error reading container output: %s", err2)
					}

					return
				}

				containerLogger.Info(line)
			}
		}()
	}

	if prelaunchFunc != nil {
		if err = prelaunchFunc(); err != nil {
			return err
		}
	}

	// start container with HostConfig was deprecated since v1.10 and removed in v1.2
	err = client.StartContainer(containerID, nil)
	if err != nil {
		dockerLogger.Errorf("start-could not start container: %s", err)
		return err
	}

	dockerLogger.Debugf("Started container %s", containerID)
	return nil
}

//Stop stops a running chaincode
func (vm *DockerVM) Stop(ctxt context.Context, ccid ccintf.CCID, timeout uint, dontkill bool, dontremove bool) error {

	// verify if matches External SCC first to remove it from the registry
	if ccid.IsSysSCC {
		path := ccid.ChaincodeSpec.ChaincodeId.Path

		ipctemplate := extSccDockerTypeRegistry[path]
		if ipctemplate == nil {
			return fmt.Errorf("%s not registered", path)
		}

		instName, _ := vm.GetVMName(ccid)

		ipc := extSccDockerInstRegistry[instName]

		if ipc == nil {
			return fmt.Errorf("%s not found", instName)
		}

		if !ipc.running {
			return fmt.Errorf("%s not running", instName)
		}

		delete(extSccDockerInstRegistry, instName)
	}

	id, err := vm.GetVMName(ccid)
	if err != nil {
		return err
	}

	client, err := vm.getClientFnc()
	if err != nil {
		dockerLogger.Debugf("stop - cannot create client %s", err)
		return err
	}
	id = strings.Replace(id, ":", "_", -1)

	err = vm.stopInternal(ctxt, client, id, timeout, dontkill, dontremove)

	return err
}

func (vm *DockerVM) stopInternal(ctxt context.Context, client dockerClient,
	id string, timeout uint, dontkill bool, dontremove bool) error {
	err := client.StopContainer(id, timeout)
	if err != nil {
		dockerLogger.Debugf("Stop container %s(%s)", id, err)
	} else {
		dockerLogger.Debugf("Stopped container %s", id)
	}
	if !dontkill {
		err = client.KillContainer(docker.KillContainerOptions{ID: id})
		if err != nil {
			dockerLogger.Debugf("Kill container %s (%s)", id, err)
		} else {
			dockerLogger.Debugf("Killed container %s", id)
		}
	}
	if !dontremove {
		err = client.RemoveContainer(docker.RemoveContainerOptions{ID: id, Force: true})
		if err != nil {
			dockerLogger.Debugf("Remove container %s (%s)", id, err)
		} else {
			dockerLogger.Debugf("Removed container %s", id)
		}
	}
	return err
}

//Destroy destroys an image
func (vm *DockerVM) Destroy(ctxt context.Context, ccid ccintf.CCID, force bool, noprune bool) error {
	id, err := vm.GetVMName(ccid)
	if err != nil {
		return err
	}

	client, err := vm.getClientFnc()
	if err != nil {
		dockerLogger.Errorf("destroy-cannot create client %s", err)
		return err
	}
	id = strings.Replace(id, ":", "_", -1)

	err = client.RemoveImageExtended(id, docker.RemoveImageOptions{Force: force, NoPrune: noprune})

	if err != nil {
		dockerLogger.Errorf("error while destroying image: %s", err)
	} else {
		dockerLogger.Debugf("Destroyed image %s", id)
	}

	return err
}

//GetVMName generates the docker image from peer information given the hashcode. This is needed to
//keep image name's unique in a single host, multi-peer environment (such as a development environment)
func (vm *DockerVM) GetVMName(ccid ccintf.CCID) (string, error) {
	name := ccid.GetName()

	//Convert to lowercase and replace any invalid characters with "-"
	r := regexp.MustCompile("[^a-zA-Z0-9-_.]")

	if ccid.NetworkID != "" && ccid.PeerID != "" {
		name = strings.ToLower(
			r.ReplaceAllString(
				fmt.Sprintf("%s-%s-%s", ccid.NetworkID, ccid.PeerID, name), "-"))
	} else if ccid.NetworkID != "" {
		name = strings.ToLower(
			r.ReplaceAllString(
				fmt.Sprintf("%s-%s", ccid.NetworkID, name), "-"))
	} else if ccid.PeerID != "" {
		name = strings.ToLower(
			r.ReplaceAllString(
				fmt.Sprintf("%s-%s", ccid.PeerID, name), "-"))
	}

	// Check name complies with Docker's repository naming rules
	r = regexp.MustCompile("^[a-z0-9]+(([._-][a-z0-9]+)+)?$")

	if !r.MatchString(name) {
		dockerLogger.Errorf("Error constructing Docker VM Name. '%s' breaks Docker's repository naming rules", name)
		return name, fmt.Errorf("Error constructing Docker VM Name. '%s' breaks Docker's repository naming rules", name)
	}
	return name, nil
}

func (vm *DockerVM) getDockerExtSCCInstance(ctxt context.Context, ipctemplate *dockerSysExtContainer, instName string, args []string, env []string) (*dockerSysExtContainer, error) {
	ec := extSccDockerInstRegistry[instName]
	if ec != nil {
		dockerLogger.Warningf("Docker ext scc chaincode instance exists for %s", instName)
		return ec, nil
	}
	ec = &dockerSysExtContainer{args: args, env: env, chaincode: ipctemplate.chaincode,
		chaincodeSpecType: ipctemplate.chaincodeSpecType, configPath: ipctemplate.configPath}
	extSccDockerInstRegistry[instName] = ec
	dockerLogger.Debugf("Docker ext scc chaincode instance created for %s", instName)
	return ec, nil
}
