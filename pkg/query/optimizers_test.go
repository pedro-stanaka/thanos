// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package query

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/promql-engine/api"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"
)

var reSpaces = strings.NewReplacer("\n", "", "\t", "")

func TestOptimizeSetProjectionLabels(t *testing.T) {
	cases := []struct {
		name     string
		expr     string
		expected string
	}{
		{
			name:     "simple vector selector",
			expr:     `metric_a{job="api-server"}`,
			expected: `metric_a{job="api-server"}[exclude()]`,
		},
		{
			name:     "top-level label_replace",
			expr:     `label_replace(kube_node_info{node="gke-1"}, "instance", "$1", "node", "(.*)")`,
			expected: `label_replace(kube_node_info{node="gke-1"}[exclude()], "instance", "$1", "node", "(.*)")`,
		},
		{
			name:     "sum by all labels",
			expr:     `sum(label_replace(kube_node_info{node="gke-1"}, "instance", "$1", "node", "(.*)"))`,
			expected: `sum(label_replace(kube_node_info{node="gke-1"}[project()], "instance", "$1", "node", "(.*)"))`,
		},
		{
			name:     "sum by target label",
			expr:     `sum by (instance) (label_replace(kube_node_info{node="gke-1"}, "instance", "$1", "node", "(.*)"))`,
			expected: `sum by (instance) (label_replace(kube_node_info{node="gke-1"}[project(instance, node)], "instance", "$1", "node", "(.*)"))`,
		},
		{
			name:     "sum not including target label",
			expr:     `sum by (node, region) (label_replace(kube_node_info{node="gke-1"}, "instance", "$1", "node", "(.*)"))`,
			expected: `sum by (node, region) (label_replace(kube_node_info{node="gke-1"}[project(node, region)], "instance", "$1", "node", "(.*)"))`,
		},
		{
			name:     "sum by source and target label",
			expr:     `sum by (node, instance, region) (label_replace(kube_node_info{node="gke-1"}, "instance", "$1", "node", "(.*)"))`,
			expected: `sum by (node, instance, region) (label_replace(kube_node_info{node="gke-1"}[project(instance, node, region)], "instance", "$1", "node", "(.*)"))`,
		},
		{
			name: "multiple label replace calls",
			expr: `
sum by (instance, node, region) (
  label_replace(
    label_replace(kube_node_info{node="gke-1"}, "ip-addr", "$1", "ip", "(.*)"),
    "instance", "$1", "node", "(.*)"
  )
)`,
			expected: `sum by (instance, node, region) (label_replace(label_replace(kube_node_info{node="gke-1"}[project(instance, node, region)], "ip-addr", "$1", "ip", "(.*)"), "instance", "$1", "node", "(.*)"))`,
		},
		{
			name:     "sum without",
			expr:     `sum without (xyz) (label_replace(kube_node_info{node="gke-1"}, "instance", "$1", "node", "(.*)"))`,
			expected: `sum without (xyz) (label_replace(kube_node_info{node="gke-1"}[exclude(xyz)], "instance", "$1", "node", "(.*)"))`,
		},
		{
			name:     "absent",
			expr:     `absent(kube_node_info{node="gke-1"})`,
			expected: `absent(kube_node_info{node="gke-1"}[project()])`,
		},
		{
			name:     "aggregation with grouping",
			expr:     `sum by (pod) (kube_node_info{node="gke-1"})`,
			expected: `sum by (pod) (kube_node_info{node="gke-1"}[project(pod)])`,
		},
		{
			name:     "double aggregation with grouping",
			expr:     `max by (pod) (sum by (pod, target) (kube_node_info{node="gke-1"}))`,
			expected: `max by (pod) (sum by (pod, target) (kube_node_info{node="gke-1"}[project(pod, target)]))`,
		},
		{
			name:     "double aggregation with by and without grouping",
			expr:     `max by (pod) (sum without (pod, target) (kube_node_info{node="gke-1"}))`,
			expected: `max by (pod) (sum without (pod, target) (kube_node_info{node="gke-1"}[exclude(pod, target)]))`,
		},
		{
			name:     "double aggregation with by and without grouping",
			expr:     `max by (pod) (sum without (target) (kube_node_info{node="gke-1"}))`,
			expected: `max by (pod) (sum without (target) (kube_node_info{node="gke-1"}[exclude(target)]))`,
		},
		{
			name:     "aggregation without grouping",
			expr:     `sum without (pod) (kube_node_info{node="gke-1"})`,
			expected: `sum without (pod) (kube_node_info{node="gke-1"}[exclude(pod)])`,
		},
		{
			name:     "aggregation with binary expression",
			expr:     `sum without (pod) (metric_a * on (node) metric_b)`,
			expected: `sum without (pod) (metric_a[exclude(pod)] * on (node) metric_b[project(__series__id, node)])`,
		},
		{
			name: "aggregation with binary expression ignoring labels",
			expr: `
max by (pod, k8s_cluster, remote_name) (
	max_over_time(prometheus_remote_storage_highest_timestamp_in_seconds[5m0s])
	- ignoring (remote_name, url) group_right ()
	max_over_time(prometheus_remote_storage_queue_highest_sent_timestamp_seconds[5m0s])
)
`,
			expected: `
max by (pod, k8s_cluster, remote_name) (
	max_over_time(prometheus_remote_storage_highest_timestamp_in_seconds[exclude(remote_name, url)][5m0s])
	- ignoring (remote_name, url) group_right ()
	max_over_time(prometheus_remote_storage_queue_highest_sent_timestamp_seconds[exclude(url)][5m0s])
)`,
		},
		{
			name:     "binary expression with vector and constant",
			expr:     `sum(metric_a * 3)`,
			expected: `sum(metric_a[project()] * 3)`,
		},
		{
			name:     "binary expression with aggregation and constant",
			expr:     `sum(metric_a) * 3`,
			expected: `sum(metric_a[project()]) * 3`,
		},
		{
			name:     "binary expression with one to one matching",
			expr:     `metric_a - metric_b`,
			expected: `metric_a[exclude()] - metric_b[exclude()]`,
		},
		{
			name:     "binary expression with one to one matching on label",
			expr:     `metric_a - on (node) metric_b`,
			expected: `metric_a[exclude()] - on (node) metric_b[project(__series__id, node)]`,
		},
		{
			name:     "binary expression with one to one matching on label group_left",
			expr:     `metric_a - on (node) group_left (cluster) metric_b`,
			expected: `metric_a[exclude()] - on (node) group_left (cluster) metric_b[project(__series__id, cluster, node)]`,
		},
		{
			name:     "binary expression with one to one matching on label group_right",
			expr:     `metric_a - on (node) group_right (cluster) metric_b`,
			expected: `metric_a[project(__series__id, cluster, node)] - on (node) group_right (cluster) metric_b[exclude()]`,
		},
		{
			name:     "aggregation with binary expression and one to one matching",
			expr:     `max by (k8s_cluster) (metric_a * up)`,
			expected: `max by (k8s_cluster) (metric_a[exclude()] * up[exclude()])`,
		},
		{
			name:     "aggregation with binary expression with one to one matching on one label",
			expr:     `max by (k8s_cluster) (metric_a * on(node) up)`,
			expected: `max by (k8s_cluster) (metric_a[project(k8s_cluster, node)] * on (node) up[project(__series__id, node)])`,
		},
		{
			name:     "aggregation with binary expression with matching one label group_left",
			expr:     `max by (k8s_cluster) (metric_a * on(node) group_left(hostname) up)`,
			expected: `max by (k8s_cluster) (metric_a[project(k8s_cluster, node)] * on (node) group_left (hostname) up[project(__series__id, hostname, node)])`,
		},
		{
			name:     "aggregation with binary expression with matching one label group_right",
			expr:     `max by (k8s_cluster) (metric_a * on(node) group_right(hostname) up)`,
			expected: `max by (k8s_cluster) (metric_a[project(__series__id, hostname, node)] * on (node) group_right (hostname) up[project(k8s_cluster, node)])`,
		},
		{
			name: "binary expression with aggregation and label replace",
			expr: `
topk(5,
    sum by (k8s_cluster) (
        max(metric_a) by (node)
        * on(node) group_right(kubernetes_io_hostname) label_replace(label_replace(label_replace(up, "node", "$1", "kubernetes_io_hostname", "(.*)"),"node_role", "$1", "role", "(.*)"), "region", "$1", "topology_kubernetes_io_region", "(.*)")
        * on(k8s_cluster) group_left(project) label_replace(k8s_cluster_info, "k8s_cluster", "$0", "cluster", ".*")
    )
)`,
			expected: `
topk(5,
	sum by (k8s_cluster) (
		max by (node) (metric_a[project(node)])
		* on (node) group_right (kubernetes_io_hostname) label_replace(label_replace(label_replace(up[project(k8s_cluster, kubernetes_io_hostname, node)], "node", "$1", "kubernetes_io_hostname", "(.*)"), "node_role", "$1", "role", "(.*)"), "region", "$1", "topology_kubernetes_io_region", "(.*)")
		* on (k8s_cluster) group_left (project) label_replace(k8s_cluster_info[project(__series__id, cluster, k8s_cluster, project)], "k8s_cluster", "$0", "cluster", ".*")))`,
		},
		{
			name: "binary expression with aggregation and label replace",
			expr: `
count by (cluster) (
    label_replace(up, "region", "$0", "region", ".*")
    * on(cluster, region) group_left(project) label_replace(max by(project, region, cluster)(k8s_cluster_info), "k8s_cluster", "$0", "cluster", ".*")
)`,
			expected: `
count by (cluster) (
	label_replace(up[project(cluster, region)], "region", "$0", "region", ".*")
	 * on (cluster, region) group_left (project) label_replace(max by (project, region, cluster) (
		k8s_cluster_info[project(cluster, project, region)]), "k8s_cluster", "$0", "cluster", ".*"))`,
		},
	}

	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(c.expr)
			require.NoError(t, err)
			plan := logicalplan.NewFromAST(expr, &query.Options{}, logicalplan.PlanOptions{})
			optimized, annos := plan.Optimize(
				[]logicalplan.Optimizer{
					SetProjectionLabels{},
					// This is a dummy optimizer that replaces VectorSelectors with a custom struct
					// which has a custom String() method.
					swapSelectors{},
				})

			require.Equal(t, annotations.Annotations{}, annos)
			require.Contains(t, normalizeSpaces(c.expected), normalizeSpaces(optimized.Root().String()))
		})
	}
}

func normalizeSpaces(s string) string {
	noSpaceStr := strings.ReplaceAll(s, " ", "")
	return reSpaces.Replace(noSpaceStr)
}

func TestOptimizeSetProjectionLabelsWithDistributedQuery(t *testing.T) {
	cases := []struct {
		name     string
		expr     string
		expected string
	}{
		{
			name: "simple vector selector",
			expr: `metric_a{job="api-server"}`,
			expected: `
dedup(
	remote(metric_a{job="api-server"}[exclude()])[0001-01-0100:00:00+0000UTC, 0001-01-0100:00:00+0000UTC],
	remote(metric_a{job="api-server"}[exclude()])[0001-01-0100:00:00+0000UTC, 0001-01-0100:00:00+0000UTC]
)`,
		},
		{
			name: "aggregation selector",
			expr: `sum by (pod) (metric_a{job="api-server"})`,
			expected: `
sum by (pod) (dedup(
	remote(sum by (pod, region) (metric_a{job="api-server"}[project(pod, region)])) [0001-01-01 00:00:00 +0000 UTC, 0001-01-0100:00:00+0000UTC],
	remote(sum by (pod, region) (metric_a{job="api-server"}[project(pod, region)])) [0001-01-01 00:00:00 +0000 UTC, 0001-01-0100:00:00+0000UTC])
)`,
		},
		{
			name: "binary expression with aggregation and label replace",
			expr: `
count by (cluster) (
    label_replace(up, "region", "$0", "region", ".*")
    * on(k8s_cluster, region) group_left(project) label_replace(k8s_cluster_info, "k8s_cluster", "$0", "cluster", ".*"))`,
			expected: `
count by (cluster) (label_replace(dedup(
	remote(up[project(cluster, k8s_cluster, region)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
	remote(up[project(cluster, k8s_cluster, region)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC]
), "region", "$0", "region", ".*") * on (k8s_cluster, region) group_left (project) dedup(
	remote(label_replace(k8s_cluster_info[project(__series__id, cluster, k8s_cluster, project, region)], "k8s_cluster", "$0", "cluster", ".*")) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
	remote(label_replace(k8s_cluster_info[project(__series__id, cluster, k8s_cluster, project, region)], "k8s_cluster", "$0", "cluster", ".*")) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC])
)`,
		},
		{
			name: "binary expression with aggregation and label replace before aggregation",
			expr: `
count by (cluster) (
    label_replace(up, "region", "$0", "region", ".*")
    * on(cluster, region) group_left(project) label_replace(max by(project, region, cluster)(k8s_cluster_info), "k8s_cluster", "$0", "cluster", ".*")
)`,
			expected: `
count by (cluster) (label_replace(
	dedup(
		remote(up[project(cluster, region)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
		remote(up[project(cluster, region)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC]), "region", "$0", "region", ".*"
) * on (cluster, region) group_left (project) label_replace(max by (project, region, cluster) (
	dedup(
		remote(max by (cluster, project, region) (k8s_cluster_info[project(cluster, project, region)])) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
		remote(max by (cluster, project, region) (k8s_cluster_info[project(cluster, project, region)])) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC])), "k8s_cluster", "$0", "cluster", ".*")
)`,
		},
		{
			name: "binary expression with matching on all labels",
			expr: `sum by (k8s_cluster) (metric_a - metric_b)`,
			expected: `
sum by (k8s_cluster) (dedup(
	remote(sum by (k8s_cluster, region) (metric_a[exclude()] - metric_b[exclude()])) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
	remote(sum by (k8s_cluster, region) (metric_a[exclude()] - metric_b[exclude()])) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC])
)`,
		},
		{
			name: "binary expression with matching on a single label",
			expr: `sum by (k8s_cluster) (metric_a - on (lbl_a) metric_b)`,
			expected: `
sum by (k8s_cluster) (dedup(
	remote(metric_a[project(k8s_cluster, lbl_a)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
	remote(metric_a[project(k8s_cluster, lbl_a)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC]
) - on (lbl_a) dedup(
	remote(metric_b[project(__series__id, lbl_a)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
	remote(metric_b[project(__series__id, lbl_a)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC])
)`,
		},
		{
			name: "binary expression with group left and matching on a single label",
			expr: `sum by (k8s_cluster) (metric_a - on (lbl_a) group_left(lbl_b) metric_b)`,
			expected: `
sum by (k8s_cluster) (dedup(
	remote(metric_a[project(k8s_cluster, lbl_a)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
	remote(metric_a[project(k8s_cluster, lbl_a)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC]
) - on (lbl_a) group_left (lbl_b) dedup(
	remote(metric_b[project(__series__id, lbl_a, lbl_b)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC],
	remote(metric_b[project(__series__id, lbl_a, lbl_b)]) [0001-01-01 00:00:00 +0000 UTC, 0001-01-01 00:00:00 +0000 UTC])
)`,
		},
	}

	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			expr, err := parser.ParseExpr(c.expr)
			require.NoError(t, err)
			plan := logicalplan.NewFromAST(expr, &query.Options{}, logicalplan.PlanOptions{})
			optimized, _ := plan.Optimize(
				[]logicalplan.Optimizer{
					logicalplan.DistributedExecutionOptimizer{
						Endpoints: api.NewStaticEndpoints([]api.RemoteEngine{
							newStubEngine([]labels.Labels{labels.FromStrings("region", "us-east")}),
							newStubEngine([]labels.Labels{labels.FromStrings("region", "us-west")}),
						}),
					},
					SetProjectionLabels{},
					// This is a dummy optimizer that replaces VectorSelectors with a custom struct
					// which has a custom String() method.
					swapSelectors{},
				})

			require.Equal(t, normalizeSpaces(c.expected), normalizeSpaces(optimized.Root().String()))
		})
	}
}

type stubEngine struct {
	labels []labels.Labels
}

func (s stubEngine) MinT() int64 { return math.MinInt64 }

func (s stubEngine) MaxT() int64 { return math.MaxInt64 }

func (s stubEngine) LabelSets() []labels.Labels { return s.labels }

func (s stubEngine) NewRangeQuery(ctx context.Context, opts promql.QueryOpts, plan api.RemoteQuery, start, end time.Time, interval time.Duration) (promql.Query, error) {
	return nil, nil
}

func newStubEngine(labels []labels.Labels) *stubEngine {
	return &stubEngine{labels: labels}
}

type swapSelectors struct{}

func (s swapSelectors) Optimize(plan logicalplan.Node, _ *query.Options) (logicalplan.Node, annotations.Annotations) {
	logicalplan.TraverseBottomUp(nil, &plan, func(_, expr *logicalplan.Node) bool {
		switch v := (*expr).(type) {
		case logicalplan.Deduplicate:
			for i := range v.Expressions {
				v.Expressions[i].Query, _ = s.Optimize(v.Expressions[i].Query, nil)
			}
		case *logicalplan.MatrixSelector:
			*expr = newMatrixOutput(v)
		case *logicalplan.VectorSelector:
			*expr = newVectorOutput(v)
		}
		return false
	})
	return plan, annotations.Annotations{}
}

type vectorOutput struct {
	*logicalplan.VectorSelector
}

func newVectorOutput(vectorSelector *logicalplan.VectorSelector) *vectorOutput {
	return &vectorOutput{
		VectorSelector: vectorSelector,
	}
}

func (vs vectorOutput) String() string {
	var projectionType string
	if vs.Projection.Include {
		projectionType = "project"
	} else {
		projectionType = "exclude"
	}
	return fmt.Sprintf("%s[%s(%s)]", vs.VectorSelector.String(), projectionType, strings.Join(vs.Projection.Labels, ", "))
}

type matrixOutput struct {
	*logicalplan.MatrixSelector
}

func newMatrixOutput(matrixSelector *logicalplan.MatrixSelector) *matrixOutput {
	return &matrixOutput{
		MatrixSelector: matrixSelector,
	}
}

func (m *matrixOutput) String() string {
	vsString := newVectorOutput(m.MatrixSelector.VectorSelector).String()
	return fmt.Sprintf("%s[%s]", vsString, m.MatrixSelector.Range)
}

var queryFunctions = map[string]*parser.Function{}

func ParseExpr(qs string) (parser.Expr, error) {
	return parser.NewParser(qs, parser.WithFunctions(queryFunctions)).ParseExpr()
}
