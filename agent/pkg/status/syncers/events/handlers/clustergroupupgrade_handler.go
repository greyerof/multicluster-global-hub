package handlers

import (
	"context"
	"errors"
	"strings"

	cguv1alpha1 "github.com/openshift-kni/cluster-group-upgrades-operator/pkg/api/clustergroupupgrades/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/stolostron/multicluster-global-hub/agent/pkg/configs"
	"github.com/stolostron/multicluster-global-hub/agent/pkg/status/filter"
	"github.com/stolostron/multicluster-global-hub/agent/pkg/status/generic"
	"github.com/stolostron/multicluster-global-hub/agent/pkg/status/interfaces"
	"github.com/stolostron/multicluster-global-hub/pkg/bundle/event"
	"github.com/stolostron/multicluster-global-hub/pkg/constants"
	"github.com/stolostron/multicluster-global-hub/pkg/database/models"
	"github.com/stolostron/multicluster-global-hub/pkg/enum"
	"github.com/stolostron/multicluster-global-hub/pkg/utils"
)

func NewClusterGroupUpgradeEventEmitter() interfaces.Emitter {
	name := strings.Replace(string(enum.CGUEventType), enum.EventTypePrefix, "", -1)
	return generic.NewGenericEmitter(enum.CGUEventType, generic.WithPostSend(
		// After sending the event, update the filter cache and clear the bundle from the handler cache.
		func(data interface{}) {
			events, ok := data.(*event.ClusterGroupUpgradeEventBundle)
			if !ok {
				return
			}
			// update the time filter: with latest event
			for _, evt := range *events {
				filter.CacheTime(name, evt.CreatedAt)
			}
			// reset the payload
			*events = (*events)[:0]
		}),
	)
}

type clusterGroupUpgradeEventHandler struct {
	ctx           context.Context
	name          string
	runtimeClient client.Client
	eventType     string
	payload       *event.ClusterGroupUpgradeEventBundle
}

func NewClusterGroupUpgradeEventHandler(ctx context.Context, c client.Client) *clusterGroupUpgradeEventHandler {
	name := strings.Replace(string(enum.CGUEventType), enum.EventTypePrefix, "", -1)
	filter.RegisterTimeFilter(name)
	return &clusterGroupUpgradeEventHandler{
		ctx:           ctx,
		name:          name,
		eventType:     string(enum.CGUEventType),
		runtimeClient: c,
		payload:       &event.ClusterGroupUpgradeEventBundle{},
	}
}

func (h *clusterGroupUpgradeEventHandler) ShouldUpdate(obj client.Object) bool {
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

func (h *clusterGroupUpgradeEventHandler) getClusterIDs(cgu *cguv1alpha1.ClusterGroupUpgrade) ([]string, error) {
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

func (h *clusterGroupUpgradeEventHandler) Update(obj client.Object) bool {
	evt, ok := obj.(*corev1.Event)
	if !ok {
		return false
	}

	cgu, err := getInvolvedCGU(h.ctx, h.runtimeClient, evt)
	if err != nil {
		log.Error(err, "failed to get involved ClusterGroupUpgrade", "event", evt.Namespace+"/"+evt.Name, "cluster", cgu.Name)
		return false
	}

	clusterIds, err := h.getClusterIDs(cgu)
	if err != nil {
		log.Error(err, "failed to get clusterIds", "event", evt.Namespace+"/"+evt.Name)
		return false
	}

	clusterEvent := models.ClusterGroupUpgradeEvent{
		EventName:           evt.Name,
		EventNamespace:      evt.Namespace,
		Message:             evt.Message,
		Reason:              evt.Reason,
		CGUName:             cgu.Name,
		ClusterIDs:          clusterIds,
		LeafHubName:         configs.GetLeafHubName(),
		ReportingController: evt.ReportingController,
		ReportingInstance:   evt.ReportingInstance,
		EventType:           evt.Type,
		CreatedAt:           getEventLastTime(evt).Time,
	}

	*h.payload = append(*h.payload, clusterEvent)
	return true
}

func (*clusterGroupUpgradeEventHandler) Delete(client.Object) bool {
	// do nothing
	return false
}

func (h *clusterGroupUpgradeEventHandler) Get() interface{} {
	return h.payload
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
