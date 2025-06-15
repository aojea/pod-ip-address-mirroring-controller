package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

const controllerAgentName = "pod-ip-controller"

// Controller holds the components for our controller
type Controller struct {
	clientset       kubernetes.Interface
	podLister       corelisters.PodLister
	podsSynced      cache.InformerSynced
	ipAddressLister networkinglisters.IPAddressLister
	ipAddressSynced cache.InformerSynced
	workqueue       workqueue.TypedRateLimitingInterface[string]
}

// NewController creates a new controller
func NewController(clientset kubernetes.Interface, factory informers.SharedInformerFactory) *Controller {
	podInformer := factory.Core().V1().Pods()
	ipAddressInformer := factory.Networking().V1().IPAddresses()

	c := &Controller{
		clientset:       clientset,
		podLister:       podInformer.Lister(),
		podsSynced:      podInformer.Informer().HasSynced,
		ipAddressLister: ipAddressInformer.Lister(),
		ipAddressSynced: ipAddressInformer.Informer().HasSynced,
		workqueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "PodIPs"},
		),
	}

	klog.Infoln("Setting up event handlers")
	// We still use event handlers to trigger reconciliation, but the core logic
	// will now do a full sync instead of processing just the single item.
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueueReconcile,
		UpdateFunc: func(old, new interface{}) { c.enqueueReconcile(new) },
		DeleteFunc: c.enqueueReconcile,
	})

	ipAddressInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueReconcile,
		UpdateFunc: func(old, new interface{}) {
			// If an IPAddress is updated, we might need to reconcile.
			c.enqueueReconcile(new)
		},
		DeleteFunc: c.enqueueReconcile,
	})

	return c
}

// enqueueReconcile triggers a full reconciliation by adding a static key to the queue.
func (c *Controller) enqueueReconcile(obj interface{}) {
	// We use a static key because the sync handler will perform a full reconciliation
	// of all pods and IPs, not just for the object that triggered the event.
	c.workqueue.Add("reconcile-all")
}

// Run starts the controller's main loop.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	klog.Infoln("Starting Pod IP controller")

	klog.Infoln("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.podsSynced, c.ipAddressSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Infoln("Starting workers")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Infoln("Started workers")
	<-stopCh
	klog.Infoln("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the queue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	key, shutdown := c.workqueue.Get()
	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(key string) error {
		defer c.workqueue.Done(key)

		// The key is now static ("reconcile-all"), so we call our main sync function.
		if err := c.reconcileAll(); err != nil {
			// We had a failure, re-queue the item to retry later.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error during reconciliation: %s, requeuing", err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(key)
		klog.Infoln("Successfully completed full reconciliation cycle")
		return nil
	}(key)

	if err != nil {
		runtime.HandleError(err)
	}

	return true
}

// reconcileAll performs a full synchronization between all Pod IPs and IPAddress objects.
func (c *Controller) reconcileAll() error {
	// 1. List all pods from the informer's cache
	pods, err := c.podLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	// 2. Build a map of required IPAddress objects based on current pods
	requiredIPs := make(map[string]*corev1.Pod)
	for _, pod := range pods {
		// We only care about pods that are running and have an IP.
		if pod.Status.PodIP != "" && pod.DeletionTimestamp == nil {
			requiredIPs[pod.Status.PodIP] = pod
		}
	}

	// 3. List all existing IPAddress objects from the informer's cache
	ipAddresses, err := c.ipAddressLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to list IPAddresses: %w", err)
	}

	// 4. Create missing IPAddress objects
	var errorList []error
	for ip, pod := range requiredIPs {
		// Check if an IPAddress object for this IP already exists
		_, err := c.ipAddressLister.Get(ip)
		if apierrors.IsNotFound(err) {
			// It doesn't exist, so create it
			newIPAddress := &networkingv1.IPAddress{
				ObjectMeta: metav1.ObjectMeta{
					Name: ip,
					OwnerReferences: []metav1.OwnerReference{
						*metav1.NewControllerRef(pod, corev1.SchemeGroupVersion.WithKind("Pod")),
					},
					Labels: map[string]string{
						"app.kubernetes.io/managed-by": controllerAgentName,
						"pod-name":                     pod.Name,
						"pod-namespace":                pod.Namespace,
					},
				},
				Spec: networkingv1.IPAddressSpec{
					ParentRef: &networkingv1.ParentReference{
						Group:     "", // Core group
						Resource:  "pods",
						Namespace: pod.Namespace,
						Name:      pod.Name,
					},
				},
			}
			log.Printf("Creating IPAddress object '%s' for Pod '%s/%s'", ip, pod.Namespace, pod.Name)
			_, createErr := c.clientset.NetworkingV1().IPAddresses().Create(context.TODO(), newIPAddress, metav1.CreateOptions{})
			if createErr != nil {
				// Don't let a single failure stop the entire loop, just log it.
				runtime.HandleError(fmt.Errorf("failed to create IPAddress %s: %w", ip, createErr))
				errorList = append(errorList, createErr)
			}
		} else if err != nil {
			runtime.HandleError(fmt.Errorf("failed to get IPAddress %s: %w", ip, err))
			errorList = append(errorList, err)
		}
	}

	// 5. Delete stale IPAddress objects.
	// The owner reference should handle most cases, but this provides self-healing.
	for _, ipAddr := range ipAddresses {
		// Only check objects we are supposed to manage
		if val, ok := ipAddr.Labels["app.kubernetes.io/managed-by"]; !ok || val != controllerAgentName {
			continue
		}

		if _, needed := requiredIPs[ipAddr.Name]; !needed {
			// This IPAddress is no longer required by any running pod.
			log.Printf("Deleting stale IPAddress object '%s'", ipAddr.Name)
			err := c.clientset.NetworkingV1().IPAddresses().Delete(context.TODO(), ipAddr.Name, metav1.DeleteOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				runtime.HandleError(fmt.Errorf("failed to delete stale IPAddress %s: %w", ipAddr.Name, err))
				errorList = append(errorList, err)
			}
		}
	}

	return errors.Join(errorList...)
}

func main() {
	var kubeconfig string
	var master string

	flag.StringVar(&kubeconfig, "kubeconfig", "", "absolute path to the kubeconfig file")
	flag.StringVar(&master, "master", "", "master url")
	flag.Parse()

	// creates the connection
	config, err := clientcmd.BuildConfigFromFlags(master, kubeconfig)
	if err != nil {
		klog.Fatal(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error building clientset: %s", err.Error())
	}

	// We set a resync period to ensure reconcileAll is called periodically,
	// even if no events are received. This makes the controller self-healing.
	factory := informers.NewSharedInformerFactory(clientset, 30*time.Second)
	controller := NewController(clientset, factory)

	stopCh := make(chan struct{})
	defer close(stopCh)

	factory.Start(stopCh)

	if err = controller.Run(1, stopCh); err != nil {
		log.Fatalf("Error running controller: %s", err.Error())
	}
}
