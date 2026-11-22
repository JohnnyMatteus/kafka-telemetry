# 0011. Autenticação de operador na API de consulta

- Status: Aceito
- Data: 2026-11-22

## Contexto

A API de consulta (ADR 0010) expõe estufas, séries, alertas, DLQ e saúde do pipeline, e aceita uma escrita: o reconhecimento de alerta. Até aqui a API estava aberta na porta de HTTP do agregador. Reconhecer um alerta precisa registrar quem agiu, e sem identidade o campo `acknowledged_by` seria um texto livre enviado pelo cliente, que qualquer um forjaria.

## Decisão

- Toda rota da API exige `Authorization: Bearer <token>`, inclusive as leituras `GET`. `/metrics` e `/healthz` do agregador continuam abertos, porque são scrapeados pelo Prometheus.
- Os tokens vêm de `OPERATOR_TOKENS` (`operador:token,...`), no mesmo padrão do `payments-events-go` (ADR 0011 daquele projeto). Cada token pertence a uma pessoa.
- O token é guardado como SHA-256 e comparado em tempo constante. Token com menos de 8 caracteres ou operador repetido derruba a partida.
- Sem `OPERATOR_TOKENS`, nenhuma rota da API é registrada, nem mesmo `/thresholds`. A API fica desligada, não aberta.
- `POST /alerts/{id}/ack` grava `acknowledged_by` com o nome do operador do token. O nome nunca vem do corpo da requisição.
- A coluna `acknowledged_by` é criada com `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` na partida, além do `init.sql`, para volumes já existentes. O sink JDBC continua sem tocar nas colunas de reconhecimento.

## Consequências

- Simples de rodar e de testar; as credenciais de desenvolvimento são `dev-only-...` no compose.
- Não há papéis (leitura x ação), expiração nem revogação: quem tem o token lê e reconhece. Aceitável para o painel de uma estufa, não para multi-tenant.
- Rotação exige trocar `OPERATOR_TOKENS` e reiniciar o agregador.
- O front-end guarda o token pela sessão do navegador e envia em todo request; um 401 encerra a sessão.
- Em produção, a troca natural é um gateway OIDC na frente da API. O `OperatorAuth` é o único ponto a trocar, porque os handlers só recebem o nome do operador.

## Alternativas

- **OIDC com PKCE direto no front-end**: melhor para produção, mas exige um provedor de identidade só para este projeto.
- **Cabeçalho `X-Operator` confiado**: qualquer cliente se passaria por outro operador e a autoria deixaria de valer.
- **Basic auth**: sem vantagem sobre o token e pior para rotação.
- **Só proteger as escritas**: deixaria estufas, séries e DLQ legíveis por qualquer um na rede; o custo de exigir token em todas as rotas é pequeno.
