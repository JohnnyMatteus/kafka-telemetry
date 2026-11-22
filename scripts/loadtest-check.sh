#!/usr/bin/env bash
# Confere o resultado de um teste de carga contra pisos absolutos e, se houver,
# contra a execução anterior (limiar de regressão).
#
#   scripts/loadtest-check.sh RESULTADO.json LIMIARES.json [BASE.json]
#
# Falha fechado: campo ausente ou nulo no resultado reprova, em vez de passar
# por zero. Sem BASE.json só valem os pisos absolutos.
set -euo pipefail

RESULT="${1:?uso: loadtest-check.sh RESULTADO.json LIMIARES.json [BASE.json]}"
LIMITS="${2:?uso: loadtest-check.sh RESULTADO.json LIMIARES.json [BASE.json]}"
BASE="${3:-}"
command -v jq >/dev/null || { echo "jq é obrigatório"; exit 2; }

fail=0
bad() { echo "REPROVADO: $*"; fail=1; }
ok() { echo "ok        : $*"; }
r() { jq -r "$1" "$RESULT"; }
l() { jq -r "$1" "$LIMITS"; }
# cmp A OP B: comparação numérica com awk (jq não tem aritmética de string).
cmp() { awk -v a="$1" -v b="$3" "BEGIN { exit !(a $2 b) }"; }

# 1. Os campos existem e são números (nada é tratado como zero).
for f in rate_requested acked errors throughput_per_s ack_p99_ms consumed lag_max drain_seconds; do
  t="$(jq -r --arg f "$f" '.[$f] | type' "$RESULT")"
  [[ "$t" == "number" ]] || bad "campo $f ausente ou não numérico no resultado ($t)"
done
[[ "$(r '.drained | type')" == "boolean" ]] || bad "campo drained ausente"
[[ $fail -eq 0 ]] || { echo "resultado incompleto: a carga não produziu as medições"; exit 1; }

rate="$(r .rate_requested)"; acked="$(r .acked)"; errors="$(r .errors)"; tput="$(r .throughput_per_s)"
p99="$(r .ack_p99_ms)"; consumed="$(r .consumed)"; lag="$(r .lag_max)"; drain="$(r .drain_seconds)"

# 2. Pisos absolutos.
min_ratio="$(l .absolute.min_throughput_ratio)"
ratio="$(awk -v t="$tput" -v r="$rate" 'BEGIN { printf "%.4f", t / r }')"
if cmp "$ratio" ">=" "$min_ratio"; then ok "vazão efetiva $tput/s (${ratio} do pedido, mínimo $min_ratio)"; else bad "vazão efetiva $tput/s é ${ratio} do pedido, mínimo $min_ratio"; fi
max_p99="$(l .absolute.max_ack_p99_ms)"
if cmp "$p99" "<=" "$max_p99"; then ok "ack p99 ${p99} ms (máximo $max_p99)"; else bad "ack p99 ${p99} ms acima de $max_p99"; fi
max_err="$(l .absolute.max_errors)"
if cmp "$errors" "<=" "$max_err"; then ok "erros de produção $errors (máximo $max_err)"; else bad "erros de produção $errors acima de $max_err"; fi
min_cons="$(l .absolute.min_consumed_ratio)"
cratio="$(awk -v c="$consumed" -v a="$acked" 'BEGIN { if (a == 0) print 0; else printf "%.4f", c / a }')"
if cmp "$cratio" ">=" "$min_cons"; then ok "consumo $consumed de $acked confirmadas ($cratio, mínimo $min_cons)"; else bad "o agregador consumiu só $consumed de $acked leituras confirmadas ($cratio, mínimo $min_cons)"; fi
max_drain="$(l .absolute.max_drain_seconds)"
if [[ "$(r .drained)" == "true" ]] && cmp "$drain" "<=" "$max_drain"; then ok "lag zerou em ${drain}s (máximo $max_drain)"; else bad "o lag não zerou a tempo (drained=$(r .drained), ${drain}s, máximo $max_drain)"; fi

# 3. Regressão contra a execução anterior.
if [[ -n "$BASE" && -s "$BASE" ]]; then
  b() { jq -r "$1" "$BASE"; }
  for f in throughput_per_s ack_p99_ms lag_max drain_seconds; do
    [[ "$(jq -r --arg f "$f" '.[$f] | type' "$BASE")" == "number" ]] || { echo "base sem $f: regressão não avaliada para ele"; }
  done
  # pct_change NOVO ANTIGO: variação percentual (positivo = subiu).
  pct() { awk -v n="$1" -v o="$2" 'BEGIN { if (o == 0) print (n == 0 ? 0 : 100000); else printf "%.2f", (n - o) / o * 100 }'; }
  bt="$(b .throughput_per_s)"; bp="$(b .ack_p99_ms)"; bl="$(b .lag_max)"; bd="$(b .drain_seconds)"
  if [[ "$bt" != "null" ]]; then
    d="$(pct "$tput" "$bt")"; lim="$(l .regression.throughput_drop_pct)"
    if cmp "$d" ">=" "-$lim"; then ok "vazão ${d}% frente à base $bt/s (queda máxima ${lim}%)"; else bad "vazão caiu ${d}% frente à base $bt/s (máximo ${lim}%)"; fi
  fi
  if [[ "$bp" != "null" ]]; then
    d="$(pct "$p99" "$bp")"; lim="$(l .regression.ack_p99_increase_pct)"
    if cmp "$d" "<=" "$lim"; then ok "ack p99 ${d}% frente à base ${bp} ms (alta máxima ${lim}%)"; else bad "ack p99 subiu ${d}% frente à base ${bp} ms (máximo ${lim}%)"; fi
  fi
  if [[ "$bl" != "null" ]]; then
    d="$(pct "$lag" "$bl")"; lim="$(l .regression.lag_max_increase_pct)"
    if cmp "$d" "<=" "$lim"; then ok "lag máximo ${d}% frente à base $bl (alta máxima ${lim}%)"; else bad "lag máximo subiu ${d}% frente à base $bl (máximo ${lim}%)"; fi
  fi
  if [[ "$bd" != "null" ]]; then
    d="$(pct "$drain" "$bd")"; lim="$(l .regression.drain_increase_pct)"
    # Duas drenagens de poucos segundos oscilam muito em termos relativos: só vale acima de 10 s.
    if cmp "$drain" "<=" 10 || cmp "$d" "<=" "$lim"; then ok "drenagem ${drain}s (base ${bd}s)"; else bad "a drenagem subiu ${d}% frente à base ${bd}s (máximo ${lim}%)"; fi
  fi
else
  echo "sem base de comparação: só os pisos absolutos valem nesta execução"
fi

exit "$fail"
