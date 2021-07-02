// +build test

package test

import (
	"fmt"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"runtime/debug"
	ctrl "sigs.k8s.io/controller-runtime"
	"testing"
)

const (
	namespace = "argo-dataflow-system"
)

var (
	restConfig             = ctrl.GetConfigOrDie()
	dynamicInterface       = dynamic.NewForConfigOrDie(restConfig)
	kubernetesInterface    = kubernetes.NewForConfigOrDie(restConfig)
	stopTestAPIPortForward func()
)

func Setup(*testing.T) {
	DeletePipelines()
	WaitForPodsToBeDeleted()

	WaitForPod("zookeeper-0")
	WaitForPod("kafka-broker-0")
	WaitForPod("nats-0")
	WaitForPod("stan-0")

	stopTestAPIPortForward = StartPortForward("testapi-0", 8378)
}

func Teardown(t *testing.T) {
	stopTestAPIPortForward()
	r := recover() // test typically panic on error, so the fail fast, we recover so we can run other tests
	if r != nil {
		t.Log(fmt.Sprintf("❌ FAIL: %v", r))
		debug.PrintStack()
		t.Fail()
	}
}
