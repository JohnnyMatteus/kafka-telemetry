package hygiene_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

type loadResult map[string]any

func goodResult() loadResult {
	return loadResult{"rate_requested": 1000, "duration": "60s", "greenhouses": 100, "acked": 60000, "errors": 0,
		"throughput_per_s": 990.5, "ack_p99_ms": 40.0, "consumed": 60000, "aggregates_emitted": 400, "lag_max": 1200,
		"drain_seconds": 3, "drained": true}
}

func (r loadResult) with(k string, v any) loadResult {
	out := loadResult{}
	for kk, vv := range r {
		out[kk] = vv
	}
	if v == nil {
		delete(out, k)
	} else {
		out[k] = v
	}
	return out
}

// loadtest-check.sh é o que decide se a carga noturna passa: reprova campo
// ausente (nunca vira zero), piso absoluto violado e regressão contra a base.
func TestLoadtestCheckGatesAbsoluteFloorsAndRegression(t *testing.T) {
	root, _ := repoFiles(t)
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("sem bash")
	}
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("sem jq: o job noturno o instala/usa no runner")
	}
	script := filepath.Join(root, "scripts", "loadtest-check.sh")
	limits := filepath.Join(root, "deploy", "loadtest", "thresholds.json")
	dir := t.TempDir()
	write := func(name string, r loadResult) string {
		b, _ := json.Marshal(r)
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	run := func(result string, base ...string) error {
		args := append([]string{script, result, limits}, base...)
		return exec.Command(bash, args...).Run()
	}

	good := write("good.json", goodResult())
	if err := run(good); err != nil {
		t.Fatalf("resultado bom reprovado: %v", err)
	}
	for name, r := range map[string]loadResult{
		"p99 ausente":           goodResult().with("ack_p99_ms", nil),
		"p99 nulo":              goodResult().with("ack_p99_ms", "null"),
		"vazão abaixo do piso":  goodResult().with("throughput_per_s", 500.0),
		"erros de produção":     goodResult().with("errors", 3),
		"consumo incompleto":    goodResult().with("consumed", 30000),
		"lag que não zerou":     goodResult().with("drained", false),
		"drenagem lenta":        goodResult().with("drain_seconds", 400),
		"p99 acima do máximo":   goodResult().with("ack_p99_ms", 5000.0),
		"sem confirmações":      goodResult().with("acked", 0),
		"campo drained ausente": goodResult().with("drained", nil),
	} {
		if err := run(write("bad.json", r)); err == nil {
			t.Errorf("%s: a verificação aprovou", name)
		}
	}

	base := write("base.json", goodResult())
	within := goodResult().with("throughput_per_s", 900.0).with("ack_p99_ms", 55.0)
	if err := run(write("within.json", within), base); err != nil {
		t.Errorf("variação dentro da tolerância reprovada: %v", err)
	}
	for name, r := range map[string]loadResult{
		"vazão caiu 20%":    goodResult().with("throughput_per_s", 792.0),
		"p99 subiu 60%":     goodResult().with("ack_p99_ms", 64.0),
		"lag máximo dobra+": goodResult().with("lag_max", 3000),
		"drenagem triplica": goodResult().with("drain_seconds", 30),
	} {
		if err := run(write("reg.json", r), base); err == nil {
			t.Errorf("%s: a regressão passou", name)
		}
	}
}
