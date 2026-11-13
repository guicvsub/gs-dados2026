# CitizenTech · Componente Dados — Pipeline do Assistente de Mobilidade por Ônibus

**Global Solution 2026.2 · Engenharia de Software 4º ano · Big Data Architecture & Data Integration**

Pipeline orquestrado (Apache Airflow) que coleta o GTFS oficial da SPTrans, trata, valida e carrega num MySQL os dados que o MVP usa para **reconhecer o letreiro do ônibus, avisar a pessoa cega ou com baixa visão e orientar a descida**. Um segundo DAG transforma os eventos de reconhecimento do app em indicadores de impacto.

## Equipe

| Integrante | RM |
|---|---|
| Guilherme Santiago da Silva | RM552321 |
| Gustavo Govea Soares | RM553842 |
| Pedro Henrique Mello Silva Alves | RM554223 |
| Gabriel Borba | RM553187 |


## Ficha Técnica do MVP


| Campo | Conteúdo |
|---|---|
| Nome do produto | Assistente de mobilidade por ônibus |
| Cidadão e território | Pessoas cegas ou com baixa visão que usam ônibus · São Paulo (SP), cidade piloto |
| Problema, em uma frase | Pessoas cegas ou com baixa visão têm dificuldade para identificar qual ônibus está chegando ao ponto e para saber quando descer. |
| Proposta de valor | Identificar o ônibus correto e orientar o trajeto com alertas por voz e vibração, ampliando autonomia e segurança. |
| Arquitetura: componentes | App web acessível (HTML/CSS/JS) · API Python · **pipeline de dados Airflow + MySQL (este repo)** · visão computacional/OCR · ESP32-CAM vestível · CI/CD e nuvem |
| Fontes de dados | GTFS estático SPTrans (via Mobility Database, `mdb-8`) · eventos de reconhecimento gravados pelo app |
| Modelos de IA utilizados | OCR/visão para ler o letreiro *[a definir pela equipe de Visão]*; este repo fornece o vocabulário de letreiros e a função de casamento |
| URL do MVP e repositório | *[a definir]* · este repositório: *[link do GitHub]* |
| Indicadores de impacto | Taxa de identificação correta da linha · alertas para o ônibus errado · tempo captura→alerta (p50/p95) · acerto à noite |
| ODS conectados | ODS 10 (redução das desigualdades), ODS 11 (meta 11.2: transporte acessível para pessoas com deficiência), ODS 9 |

## O que o pipeline entrega ao produto

| Pergunta que o app precisa responder | Tabela / artefato |
|---|---|
| "Este ônibus que chegou é o meu?" | `letreiro` (2.268 textos reais de painel) + `pipeline/matching.py` |
| "E se não tiver internet no ponto?" | `data/serving/letreiros.json` (536 KB, guardado no celular) |
| "Quando eu desço?" | `itinerario` (sequência de paradas com tempo e distância) + `parada` |
| "Quanto tempo até o próximo?" (sem tempo real) | `frequencia` (intervalo por faixa horária) |
| "O produto está funcionando para as pessoas?" | `evento_reconhecimento` → `indicador_reconhecimento_diario` |
| "Qual versão dos dados está no ar?" | `carga_controle` (sha256 do feed, status, contagens) |

## Arquitetura

![Arquitetura do pipeline](docs/arquitetura.svg)

- **Raw (imutável):** o zip original, identificado pelo SHA-256. Permite reprocessar sem baixar de novo.
- **Staging (Parquet):** tabelas tipadas e normalizadas por execução.
- **Portão de qualidade:** 18 checagens (volume, PK, FK, coordenadas na RMSP, tempos crescentes, cobertura). Se uma crítica falha, nada é carregado e o MVP segue com a última versão boa.
- **Curada (MySQL):** carga em **uma transação** com reconciliação staging × banco. Reexecutar com o mesmo feed **não duplica nada** (o DAG pula por short-circuit).
- **Entrega:** banco para a API + pacote offline para o app.

Dicionário de dados completo: [docs/dicionario_de_dados.md](docs/dicionario_de_dados.md) · DDL: [sql/schema_mysql.sql](sql/schema_mysql.sql)

## Como executar

### Opção A — Airflow completo com Docker (recomendado para a avaliação)

Requisitos: Docker Desktop (ou Docker Engine + Compose v2).

```bash
cp .env.example .env          # troque as senhas
docker compose up -d --build  # sobe MySQL, Postgres, Airflow webserver e scheduler
```

1. Abra http://localhost:8080 e entre com `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` do `.env`.
2. Ative e dispare `citizentech_gtfs_sptrans` (▶ Trigger DAG).
3. Ative e dispare `citizentech_indicadores_reconhecimento` com a configuração `{"simular": true}` para gerar dados de demonstração.
4. Veja os dados no MySQL (porta 3306 só em `127.0.0.1`):

```bash
docker compose exec mysql mysql -u citizentech -p citizentech -e "SELECT * FROM carga_controle;"
```

### Opção B — Sem Docker (mesmas funções dos DAGs, banco SQLite local)

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt        # Windows: .venv\Scripts\pip
.venv/bin/python -m pipeline.run_local tudo      # GTFS + simulação + indicadores
.venv/bin/python -m pipeline.run_local gtfs      # de novo: status "ignorada" (idempotente)
.venv/bin/python -m pytest -q                    # 10 testes
.venv/bin/python -m scripts.gerar_documentacao   # dicionário + consultas de verificação
```

Para usar MySQL em vez de SQLite: `CITIZENTECH_DB_URL=mysql+pymysql://usuario:senha@host:3306/citizentech`.

### Opção C — GitHub Actions

O workflow [.github/workflows/pipeline.yml](.github/workflows/pipeline.yml) roda a cada push: testes, carga dos DAGs no Airflow 2.10.5, `airflow dags test` dos dois DAGs contra um MySQL 8.4, reexecução para provar a idempotência e conferência das contagens. Logs e `letreiros.json` ficam como artefato `evidencias-pipeline`.

## Estrutura

```
dags/                       DAGs do Airflow (só orquestram; a lógica fica em pipeline/)
pipeline/
  extract.py                ingestão raw + sha256
  transform.py              GTFS -> linha, letreiro, parada, itinerario, frequencia (Parquet)
  quality.py                portão de qualidade
  load.py                   carga transacional + linhagem
  serve.py                  pacote offline do app
  matching.py               contrato com a visão computacional (OCR -> letreiro)
  metrics.py                DAG 2: indicadores de impacto
  simulacao.py              eventos simulados com premissas documentadas
  schema.py                 modelo físico (fonte do DDL e do dicionário)
  run_local.py              executor sem Airflow
sql/schema_mysql.sql        DDL gerado do schema
docs/                       arquitetura, dicionário, evidências, PDF de entrega
tests/                      testes com mini-GTFS sintético
```

## Dados: origem, licença e decisões

- **GTFS SPTrans** — publicado pela SPTrans; o download oficial exige cadastro, por isso usamos o espelho público do [Mobility Database](https://mobilitydatabase.org) (`mdb-8`), que replica cada versão publicada. Versão carregada nas evidências: sha256 `457293b83e34…` (catálogo atualizado em 10/11/2026). A URL é configurável (`GTFS_URL`) para outra cidade piloto.
- **`shapes.txt` (60 MB)** fica só no raw: o app não desenha o traçado, só precisa da sequência de paradas.
- **Uma viagem de referência por linha e sentido:** a SPTrans publica uma viagem por sentido e modela os horários em `frequencies.txt`; por isso não há grade horária completa, e a espera é uma **estimativa**.
- **Tempo real (Olho Vivo)** não está no MVP: exige token e não é necessário para o fluxo central. É o próximo passo natural.
- **Eventos simulados:** enquanto não há testes de campo, o DAG 2 pode gerar eventos marcados `origem = 'simulado'`, com premissas explícitas em [pipeline/simulacao.py](pipeline/simulacao.py). **Esses números não são resultado do produto** e não devem aparecer no pitch como medição.

## Segurança e LGPD

- Nenhuma credencial no código: tudo via `.env` (no `.gitignore`) ou GitHub Secrets; `.env.example` só tem valores de exemplo.
- MySQL publicado apenas em `127.0.0.1`.
- `evento_reconhecimento` foi desenhada para **não conter dado pessoal**: sem imagem, sem coordenada, sem nome ou telefone. A sessão é um hash SHA-256 de um ID efêmero gerado no aparelho. Imagens são processadas no celular e descartadas.
- Dados GTFS são públicos e não pessoais.

## Limitações conhecidas

- Sem posição dos ônibus em tempo real: a espera vem da frequência planejada.
- `distancia_proxima_m` é em linha reta entre paradas (subestima a distância pela rua); serve para antecipar o alerta, não para navegação.
- Linhas com destino idêntico e código parecido (ex.: `8000-1` e `8000-10`) só se distinguem pelo sufixo: o casamento marca `ambiguo = true` e o app não deve confirmar o ônibus nesse caso.
