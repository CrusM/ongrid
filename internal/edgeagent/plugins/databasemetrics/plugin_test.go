package databasemetrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ongridio/ongrid/internal/edgeagent/plugins"
	"github.com/ongridio/ongrid/internal/edgeagent/plugins/metricscommon"
	"github.com/ongridio/ongrid/internal/pkg/tunnel"
)

type fakeDatabasePusher struct {
	mu    sync.Mutex
	calls []tunnel.PushPromSamplesRequest
}

func (f *fakeDatabasePusher) Call(_ context.Context, method string, req, resp any) error {
	if method != tunnel.MethodPushPromSamples {
		return nil
	}
	in := req.(tunnel.PushPromSamplesRequest)
	f.mu.Lock()
	f.calls = append(f.calls, in)
	f.mu.Unlock()
	if out, ok := resp.(*tunnel.PushPromSamplesResponse); ok {
		out.Accepted = len(in.Samples)
	}
	return nil
}

func (f *fakeDatabasePusher) snapshot() []tunnel.PushPromSamplesRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]tunnel.PushPromSamplesRequest, len(f.calls))
	copy(out, f.calls)
	return out
}

func TestDatabaseMetricsPushesSyntheticUpSamples(t *testing.T) {
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(`# TYPE mysql_up gauge
mysql_up 1
`))
	}))
	t.Cleanup(okSrv.Close)
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(failSrv.Close)

	pusher := &fakeDatabasePusher{}
	p := New("", t.TempDir(), pusher, func() uint64 { return 42 }, nil)
	okSource := sourceSpec{ID: "mysql-ok", Name: "MySQL OK", DBType: "mysql", Timeout: time.Second, SourceLabel: "db:mysql-ok"}
	badSource := sourceSpec{ID: "mysql-bad", Name: "MySQL Bad", DBType: "mysql", Timeout: time.Second, SourceLabel: "db:mysql-bad"}
	p.scrapeAndPush(context.Background(), okSource, metricscommon.Target{
		ID:          okSource.ID,
		Name:        okSource.Name,
		URL:         okSrv.URL + "/metrics",
		Timeout:     time.Second,
		SourceLabel: okSource.SourceLabel,
		ExtraLabels: map[string]string{"db_type": "mysql"},
		Kind:        "mysql",
	})
	p.scrapeAndPush(context.Background(), badSource, metricscommon.Target{
		ID:          badSource.ID,
		Name:        badSource.Name,
		URL:         failSrv.URL + "/metrics",
		Timeout:     time.Second,
		SourceLabel: badSource.SourceLabel,
		ExtraLabels: map[string]string{"db_type": "mysql"},
		Kind:        "mysql",
	})

	calls := pusher.snapshot()
	if len(calls) != 2 {
		t.Fatalf("push calls = %d, want 2: %#v", len(calls), calls)
	}
	assertDatabaseUpSample(t, calls[0], "db:mysql-ok", "mysql-ok", 1)
	assertDatabaseUpSample(t, calls[1], "db:mysql-bad", "mysql-bad", 0)
	if len(calls[1].Samples) != 1 {
		t.Fatalf("failed scrape samples = %d, want only synthetic up sample", len(calls[1].Samples))
	}
}

func TestDatabaseMetricsRunClosesStartScopedStoppedChannel(t *testing.T) {
	p := New("", t.TempDir(), nil, nil, nil)
	stopped := make(chan struct{})
	current := make(chan struct{})
	p.stoppedCh = current

	p.run(context.Background(), plugins.PluginConfig{}, stopped)

	select {
	case <-stopped:
	default:
		t.Fatal("run did not close start-scoped stopped channel")
	}
	select {
	case <-current:
		t.Fatal("run closed current p.stoppedCh instead of start-scoped channel")
	default:
	}
}

func assertDatabaseUpSample(t *testing.T, req tunnel.PushPromSamplesRequest, source, targetID string, value float64) {
	t.Helper()
	if req.Source != source {
		t.Fatalf("source = %q, want %q", req.Source, source)
	}
	for _, sample := range req.Samples {
		if sample.Name == metricscommon.ScrapeUpMetricName && sample.Labels["target_id"] == targetID {
			if sample.Value != value {
				t.Fatalf("up value for %s = %v, want %v", targetID, sample.Value, value)
			}
			if sample.Labels["plugin"] != Name {
				t.Fatalf("up plugin label = %q, want %q", sample.Labels["plugin"], Name)
			}
			if sample.Labels["db_type"] != "mysql" {
				t.Fatalf("up db_type label = %q, want mysql", sample.Labels["db_type"])
			}
			return
		}
	}
	t.Fatalf("missing up sample for target %q in %#v", targetID, req.Samples)
}
