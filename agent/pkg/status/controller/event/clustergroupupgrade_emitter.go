package event

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/go-logr/logr"
	cguv1alpha1 "github.com/openshift-kni/cluster-group-upgrades-operator/pkg/api/clustergroupupgrades/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/stolostron/multicluster-global-hub/agent/pkg/status/controller/config"
	"github.com/stolostron/multicluster-global-hub/agent/pkg/status/controller/filter"
	"github.com/stolostron/multicluster-global-hub/agent/pkg/status/controller/generic"
	"github.com/stolostron/multicluster-global-hub/pkg/bundle/event"
	"github.com/stolostron/multicluster-global-hub/pkg/bundle/version"
	"github.com/stolostron/multicluster-global-hub/pkg/constants"
	"github.com/stolostron/multicluster-global-hub/pkg/database/models"
	"github.com/stolostron/multicluster-global-hub/pkg/enum"
	"github.com/stolostron/multicluster-global-hub/pkg/utils"
)

var _ generic.ObjectEmitter = &clusterGroupUpgradeEmitter{}

type clusterGroupUpgradeEmitter struct {
	ctx             context.Context
	name            string
	log             logr.Logger
	runtimeClient   client.Client
	eventType       string
	topic           string
	currentVersion  *version.Version
	lastSentVersion version.Version
	payload         event.ClusterGroupUpgradeEventBundle
}

func NewClusterGroupUpgradeEventEmitter(ctx context.Context, c client.Client, topic string) *clusterGroupUpgradeEmitter {
	name := strings.Replace(string(enum.CGUEventType), enum.EventTypePrefix, "", -1)
	filter.RegisterTimeFilter(name)
	return &clusterGroupUpgradeEmitter{
		ctx:             ctx,
		name:            name,
		log:             ctrl.Log.WithName(name),
		eventType:       string(enum.CGUEventType),
		topic:           topic,
		runtimeClient:   c,
		currentVersion:  version.NewVersion(),
		lastSentVersion: *version.NewVersion(),
		payload:         make([]models.ClusterGroupUpgradeEvent, 0),
	}
}

func (h *clusterGroupUpgradeEmitter) PostUpdate() {
	h.currentVersion.Incr()
}

func (h *clusterGroupUpgradeEmitter) ShouldUpdate(obj client.Object) bool {
	evt, ok := obj.(*corev1.Event)
	if !ok {
		return false
	}

	if evt.InvolvedObject.Kind != constants.ClusterGroupUpgradeKind {
		return false
	}

	// if it's a older event, then return false
	if !filter.Newer(h.name, getEventLastTime(evt).Time) {
		return false
	}

	return true
}

func (h *clusterGroupUpgradeEmitter) getClusterIDs(cgu *cguv1alpha1.ClusterGroupUpgrade) ([]string, error) {
	ids := make([]string, len(cgu.Status.Clusters))
	for i, cluster := range cgu.Status.Clusters {
		clusterId, err := utils.GetClusterId(h.ctx, h.runtimeClient, cluster.Name)
		if err != nil {
			return nil, errors.New("failed to get clusterId for cluster " + cluster.Name)
		}
		ids[i] = clusterId
	}

	return ids, nil
}

func (h *clusterGroupUpgradeEmitter) Update(obj client.Object) bool {
	evt, ok := obj.(*corev1.Event)
	if !ok {
		return false
	}

	cgu, err := getInvolvedCGU(h.ctx, h.runtimeClient, evt)
	if err != nil {
		h.log.Error(err, "failed to get involved ClusterGroupUpgrade", "event", evt.Namespace+"/"+evt.Name, "cluster", cgu.Name)
		return false
	}

	clusterIds, err := h.getClusterIDs(cgu)
	if err != nil {
		h.log.Error(err, "failed to get clusterIds", "event", evt.Namespace+"/"+evt.Name)
		return false
	}

	clusterEvent := models.ClusterGroupUpgradeEvent{
		EventName:           evt.Name,
		EventNamespace:      evt.Namespace,
		Message:             evt.Message,
		Reason:              evt.Reason,
		CGUName:             cgu.Name,
		ClusterIDs:          clusterIds,
		LeafHubName:         config.GetLeafHubName(),
		ReportingController: evt.ReportingController,
		ReportingInstance:   evt.ReportingInstance,
		EventType:           evt.Type,
		CreatedAt:           getEventLastTime(evt).Time,
	}

	h.payload = append(h.payload, clusterEvent)
	return true
}

func (*clusterGroupUpgradeEmitter) Delete(client.Object) bool {
	// do nothing
	return false
}

func (h *clusterGroupUpgradeEmitter) ToCloudEvent() (*cloudevents.Event, error) {
	if len(h.payload) < 1 {
		return nil, fmt.Errorf("the cloudevent instance shouldn't be nil")
	}
	e := cloudevents.NewEvent()
	e.SetType(h.eventType)
	e.SetSource(config.GetLeafHubName())
	e.SetExtension(version.ExtVersion, h.currentVersion.String())
	err := e.SetData(cloudevents.ApplicationJSON, h.payload)
	return &e, err
}

// to assert whether emit the current cloudevent
func (h *clusterGroupUpgradeEmitter) ShouldSend() bool {
	return h.currentVersion.NewerThan(&h.lastSentVersion)
}

func (h *clusterGroupUpgradeEmitter) Topic() string {
	return h.topic
}

func (h *clusterGroupUpgradeEmitter) PostSend() {
	// update the time filter: with latest event
	for _, evt := range h.payload {
		filter.CacheTime(h.name, evt.CreatedAt)
	}
	// update version and clean the cache
	h.payload = make([]models.ClusterGroupUpgradeEvent, 0)
	// 1. the version get into the next generation
	// 2. set the lastSenteVersion to current version
	h.currentVersion.Next()
	h.lastSentVersion = *h.currentVersion
}

func getInvolvedCGU(ctx context.Context, c client.Client, evt *corev1.Event) (*cguv1alpha1.ClusterGroupUpgrade, error) {
	cluster := &cguv1alpha1.ClusterGroupUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Name:      evt.InvolvedObject.Name,
			Namespace: evt.InvolvedObject.Namespace,
		},
	}
	err := c.Get(ctx, client.ObjectKeyFromObject(cluster), cluster)
	return cluster, err
}
