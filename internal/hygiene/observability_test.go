package hygiene_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/JohnnyMatteus/kafka-telemetry/internal/observability"
)

var metricRef = regexp.MustCompile(`telemetry_[a-z_]+`)

// Os arquivos de código-fonte são a fonte dos nomes: dashboards e regras só
// podem citar métricas que o código registra.
func registeredNames(t *testing.T, root string) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	re := regexp.MustCompile(`Name:\s*"(telemetry_[a-z_]+)"`)
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		b, _ := os.ReadFile(p)
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			names[m[1]] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return names
}

func baseMetric(n string) string {
	for _, suf := range []string{"_bucket", "_sum", "_count"} {
		n = strings.TrimSuffix(n, suf)
	}
	return n
}

func grafanaFiles(t *testing.T, root string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, "deploy", "grafana", "dashboards", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("nenhum dashboard versionado: %v", err)
	}
	return files
}

// Todo dashboard e toda regra só cita métrica que existe no código, e toda
// série slo:* usada num dashboard é produzida por uma regra de gravação.
func TestDashboardsAndRulesOnlyReferenceMetricsThatExist(t *testing.T) {
	root, _ := repoFiles(t)
	known := registeredNames(t, root)
	rules, err := filepath.Glob(filepath.Join(root, "deploy", "prometheus", "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var texts = map[string]string{}
	for _, f := range append(grafanaFiles(t, root), rules...) {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		texts[f] = string(b)
	}
	recorded := map[string]bool{}
	rec := regexp.MustCompile(`record:\s*(slo:[A-Za-z0-9_:]+)`)
	for _, txt := range texts {
		for _, m := range rec.FindAllStringSubmatch(txt, -1) {
			recorded[m[1]] = true
		}
	}
	slo := regexp.MustCompile(`slo:[A-Za-z0-9_:]+`)
	for f, txt := range texts {
		for _, n := range metricRef.FindAllString(txt, -1) {
			if !known[baseMetric(n)] {
				t.Errorf("%s cita %s, que nenhum código registra", filepath.Base(f), n)
			}
		}
		if strings.HasSuffix(f, ".json") {
			for _, n := range slo.FindAllString(txt, -1) {
				if !recorded[n] {
					t.Errorf("%s usa %s, que nenhuma regra de gravação produz", filepath.Base(f), n)
				}
			}
		}
	}
}

// O bucket citado nas regras do SLO de latência existe no histograma de verdade.
func TestLatencySLOBucketExistsInTheHistogram(t *testing.T) {
	root, _ := repoFiles(t)
	b, err := os.ReadFile(filepath.Join(root, "deploy", "prometheus", "slo.yml"))
	if err != nil {
		t.Fatal(err)
	}
	cited := map[string]bool{}
	for _, m := range regexp.MustCompile(`end_to_end_latency_seconds_bucket\{le="([^"]+)"\}`).FindAllStringSubmatch(string(b), -1) {
		cited[m[1]] = true
	}
	if len(cited) == 0 {
		t.Fatal("as regras do SLO de latência não citam bucket")
	}
	reg := prometheus.NewRegistry()
	observability.NewAggregatorMetrics(reg).EndToEndLatency(time.Second)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, f := range families {
		if f.GetName() != "telemetry_aggregator_end_to_end_latency_seconds" {
			continue
		}
		for _, bk := range f.GetMetric()[0].GetHistogram().GetBucket() {
			have[strconv.FormatFloat(bk.GetUpperBound(), 'f', -1, 64)] = true
		}
	}
	for le := range cited {
		if !have[le] {
			keys := make([]string, 0, len(have))
			for k := range have {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			t.Errorf("le=%q não existe no histograma (buckets: %v)", le, keys)
		}
	}
}

// Estrutura mínima dos dashboards: JSON válido, uid único, ids de painel únicos
// e toda consulta apontando para o datasource provisionado.
func TestDashboardsAreWellFormed(t *testing.T) {
	root, _ := repoFiles(t)
	uids := map[string]string{}
	for _, f := range grafanaFiles(t, root) {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var d struct {
			UID    string `json:"uid"`
			Title  string `json:"title"`
			Panels []struct {
				ID      int    `json:"id"`
				Title   string `json:"title"`
				Targets []struct {
					Expr       string `json:"expr"`
					Datasource struct {
						UID string `json:"uid"`
					} `json:"datasource"`
				} `json:"targets"`
			} `json:"panels"`
		}
		if err := json.Unmarshal(b, &d); err != nil {
			t.Fatalf("%s: %v", filepath.Base(f), err)
		}
		if d.UID == "" || d.Title == "" || len(d.Panels) == 0 {
			t.Errorf("%s: uid, título e painéis são obrigatórios", filepath.Base(f))
		}
		if other, dup := uids[d.UID]; dup {
			t.Errorf("uid %q repetido em %s e %s", d.UID, other, filepath.Base(f))
		}
		uids[d.UID] = filepath.Base(f)
		ids := map[int]bool{}
		for _, p := range d.Panels {
			if ids[p.ID] {
				t.Errorf("%s: id de painel %d repetido", filepath.Base(f), p.ID)
			}
			ids[p.ID] = true
			for _, tg := range p.Targets {
				if tg.Expr == "" || tg.Datasource.UID != "prometheus" {
					t.Errorf("%s: painel %q com consulta vazia ou sem o datasource prometheus", filepath.Base(f), p.Title)
				}
			}
		}
	}
	ds, err := os.ReadFile(filepath.Join(root, "deploy", "grafana", "provisioning", "datasources", "prometheus.yml"))
	if err != nil || !strings.Contains(string(ds), "uid: prometheus") {
		t.Errorf("o datasource provisionado precisa ter uid prometheus: %v", err)
	}
}
