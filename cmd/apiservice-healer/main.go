package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/k3s-io/k3s/pkg/apiservicehealer"
	apiregistration "github.com/rancher/wrangler/v3/pkg/generated/controllers/apiregistration.k8s.io"
	"github.com/rancher/wrangler/v3/pkg/generated/controllers/core"
	"github.com/rancher/wrangler/v3/pkg/start"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
)

func main() {
	var (
		kubeconfigFlag            string
		maxDetachDurationFlag     time.Duration
		pollIntervalFlag          time.Duration
		nonStorableGroupsFlag     string
		stateConfigMapNamespace   string
		stateConfigMapName        string
		debugFlag                 bool
	)

	flag.StringVar(&kubeconfigFlag, "kubeconfig", os.Getenv("KUBECONFIG"), "Path to a kubeconfig. Only required if out-of-cluster.")
	flag.DurationVar(&maxDetachDurationFlag, "max-detach-duration", apiservicehealer.DefaultMaxDetachDuration, "Maximum time an APIService may remain detached.")
	flag.DurationVar(&pollIntervalFlag, "poll-interval", apiservicehealer.DefaultPollInterval, "Interval between periodic sweep passes.")
	flag.StringVar(&nonStorableGroupsFlag, "non-storable-api-groups", "", "Comma-separated list of non-storable virtual API groups permitted to be detached.")
	flag.StringVar(&stateConfigMapNamespace, "state-configmap-namespace", apiservicehealer.DefaultStateConfigMapNamespace, "Namespace for state persistence ConfigMap.")
	flag.StringVar(&stateConfigMapName, "state-configmap-name", apiservicehealer.DefaultStateConfigMapName, "Name for state persistence ConfigMap.")
	flag.BoolVar(&debugFlag, "debug", false, "Enable debug logging.")
	flag.Parse()

	if debugFlag || os.Getenv("DEBUG") == "true" {
		logrus.SetLevel(logrus.DebugLevel)
	}

	// Environment variable overrides
	if envDuration := os.Getenv("MAX_DETACH_DURATION"); envDuration != "" {
		if d, err := time.ParseDuration(envDuration); err == nil {
			maxDetachDurationFlag = d
		}
	}
	if envPoll := os.Getenv("POLL_INTERVAL"); envPoll != "" {
		if d, err := time.ParseDuration(envPoll); err == nil {
			pollIntervalFlag = d
		}
	}
	if envNS := os.Getenv("STATE_CONFIGMAP_NAMESPACE"); envNS != "" {
		stateConfigMapNamespace = envNS
	}
	if envName := os.Getenv("STATE_CONFIGMAP_NAME"); envName != "" {
		stateConfigMapName = envName
	}
	if envGroups := os.Getenv("NON_STORABLE_API_GROUPS"); envGroups != "" && nonStorableGroupsFlag == "" {
		nonStorableGroupsFlag = envGroups
	}

	var customGroups []string
	if nonStorableGroupsFlag != "" {
		for _, g := range strings.Split(nonStorableGroupsFlag, ",") {
			if trimmed := strings.TrimSpace(g); trimmed != "" {
				customGroups = append(customGroups, trimmed)
			}
		}
	}

	cfg := apiservicehealer.Config{
		NonStorableGroups:       customGroups,
		MaxDetachDuration:       maxDetachDurationFlag,
		PollInterval:            pollIntervalFlag,
		StateConfigMapNamespace: stateConfigMapNamespace,
		StateConfigMapName:      stateConfigMapName,
	}
	cfg.Complete()

	logrus.Infof("Starting %s controller...", apiservicehealer.ControllerName)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	restConfig, err := buildRESTConfig(kubeconfigFlag)
	if err != nil {
		logrus.Fatalf("Failed to build Kubernetes REST config: %v", err)
	}
	restConfig.UserAgent = apiservicehealer.ControllerName

	k8sClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		logrus.Fatalf("Failed to initialize Kubernetes clientset: %v", err)
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: k8sClient.CoreV1().Events(cfg.StateConfigMapNamespace),
	})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{
		Component: apiservicehealer.ControllerName,
	})

	coreFactory := core.NewFactoryFromConfigOrDie(restConfig)
	apiRegFactory := apiregistration.NewFactoryFromConfigOrDie(restConfig)

	_, err = apiservicehealer.Register(
		ctx,
		coreFactory.Core().V1().Namespace(),
		apiRegFactory.Apiregistration().V1().APIService(),
		k8sClient,
		recorder,
		cfg,
	)
	if err != nil {
		logrus.Fatalf("Failed to register %s controller: %v", apiservicehealer.ControllerName, err)
	}

	if err := start.All(ctx, 2, coreFactory, apiRegFactory); err != nil {
		logrus.Fatalf("Failed to start informers: %v", err)
	}

	logrus.Infof("%s controller running successfully", apiservicehealer.ControllerName)
	<-ctx.Done()
	logrus.Infof("Shutting down %s controller", apiservicehealer.ControllerName)
}

func buildRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
