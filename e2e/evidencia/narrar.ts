import type { Page } from '@playwright/test';

export interface Chamada {
  metodo: string;
  url: string;
  status?: number;
  corpo?: unknown;
}

export interface Detalhe {
  descricao: string;
  chamada?: Chamada;
  // Saída de terminal ou de consulta, exibida como bloco monoespaçado.
  saida?: string;
}

const escapar = (s: string) =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const formatar = (corpo: unknown) =>
  typeof corpo === 'string' ? corpo : JSON.stringify(corpo, null, 2);

// Cartão grande com o passo, a explicação e a chamada/saída real.
export async function narrar(page: Page, titulo: string, detalhe: Detalhe, pausaMs = 2800) {
  const { descricao, chamada, saida } = detalhe;
  const statusCor = chamada?.status && chamada.status >= 400 ? '#f87171' : '#4ade80';
  const blocoChamada = chamada
    ? `<div class="req"><span class="metodo">${escapar(chamada.metodo)}</span> ${escapar(chamada.url)}
       ${chamada.status !== undefined ? `<span class="status" style="color:${statusCor}">${chamada.status}</span>` : ''}</div>
       ${chamada.corpo !== undefined ? `<pre>${escapar(formatar(chamada.corpo))}</pre>` : ''}`
    : '';
  const blocoSaida = saida ? `<pre>${escapar(saida)}</pre>` : '';
  // O destaque do locator.highlight() continua ativo e casaria com o texto do cartão.
  await page.hideHighlight();
  await page.setContent(`<!doctype html><html><head><meta charset="utf-8"><style>
    body { margin:0; height:100vh; display:flex; align-items:center; justify-content:center;
      background:#0f172a; color:#e2e8f0; font-family: -apple-system, 'Segoe UI', Roboto, sans-serif; }
    .card { width:1160px; max-height:660px; overflow:hidden; background:#1e293b; border-radius:18px;
      padding:32px 40px; box-shadow:0 20px 60px rgba(0,0,0,.5); }
    h1 { margin:0 0 12px; font-size:34px; color:#fbbf24; }
    p { margin:0 0 18px; font-size:21px; line-height:1.45; white-space:pre-line; }
    .req { font-family: Menlo, monospace; font-size:17px; margin-bottom:8px; }
    .metodo { background:#2563eb; padding:2px 8px; border-radius:6px; font-weight:bold; }
    .status { font-weight:bold; margin-left:10px; }
    pre { background:#0b1220; padding:14px 18px; border-radius:10px; font-size:14px; line-height:1.35;
      max-height:400px; overflow:hidden; white-space:pre-wrap; word-break:break-all; margin:0; }
  </style></head><body><div class="card"><h1>${escapar(titulo)}</h1><p>${escapar(descricao)}</p>
  ${blocoChamada}${blocoSaida}</div></body></html>`);
  await page.waitForTimeout(pausaMs);
}
