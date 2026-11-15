# 💳 Detetor de Fraudes em Tempo Real - CDC com Confluent Cloud & Flink SQL

Este projeto é a resolução do desafio final do bootcamp **IBM Confluent - Dados em tempo real para agentes de IA** da DIO. Ele implementa um pipeline de streaming em tempo real, capturando transações financeiras de um banco de dados PostgreSQL e aplicando regras antifraude através do Apache Flink.

## 🎯 Objetivo do Projeto

Analisar transações de cartões de crédito em tempo real e disparar um alerta (criar uma mensagem num tópico Kafka) sempre que o sistema detetar **mais de 3 transações no mesmo cartão num intervalo de 60 segundos**.

## 🛠️ Tecnologias Utilizadas

*   **Neon DB (PostgreSQL):** Banco de dados relacional (Source).
*   **Docker (psql):** Ferramenta CLI para interagir com o banco e simular as transações.
*   **Confluent Cloud:**
    *   **Kafka:** Mensageria.
    *   **Debezium (CDC - Change Data Capture):** Conector para ler o Log do Postgres.
    *   **Schema Registry:** Gestão de schemas (AVRO).
    *   **Flink SQL:** Motor de processamento de streams (Complex Event Processing - CEP).

---

## 🚀 Passo a Passo (O Guia de Sobrevivência)

### Fase 1: Preparação do Banco de Dados (Neon)

1. Criei um projeto no [Neon.tech](https://neon.tech/) na região `us-east-1` (AWS).
2. Usei o Docker localmente para executar o comando `psql` e carregar os meus ficheiros `.sql`:

**Comando de Inserção de Tabelas (DDL):**
```bash
docker run --rm -v "$PWD/sql:/sql:ro" postgres:16-alpine psql "postgres://user:***@host/dbname?sslmode=require" -f /sql/01_ddl.sql
```

**Comando de Inserção de Dados (Seed):**
```bash
docker run --rm -v "$PWD/sql:/sql:ro" postgres:16-alpine psql "postgres://user:***@host/dbname?sslmode=require" -f /sql/02_seed.sql
```

![Criação da tabela no banco Neon](prints/neon_tables.png)

### Fase 2: O Conector CDC (E as lições aprendidas)

1. No Confluent Cloud, criei um cluster e adicionei o conector **Postgres CDC Source V2 (Debezium)**.
2. Configurei as credenciais do Neon e defini as tabelas alvo (`payments.accounts`, `payments.cards`, `payments.transactions`).

> ⚠️ **A Primeira Tentativa (Erro do JSON)**
> Na primeira vez, configurei os campos `output.data.format` e `output.key.format` como `JSON`. Isso fez com que o Flink **não** reconhecesse a restrição `NOT NULL` do banco original, impedindo a criação de *Primary Keys* e travando todo o processo de *changelog* no Flink.

> ⚠️ **A Segunda Tentativa (Erro de Schema Incompatível)**
> Apaguei o conector JSON e tentei criar o AVRO com o mesmo nome (`neon-desafio`). O conector falhou (Crashed) porque o **Schema Registry** ainda guardava o "fantasma" do esquema JSON antigo.

![Erro do Conector por conflito no Schema Registry](prints/confluent_erro.png)

**A Solução (Fase 3):**
Tive que fazer a limpeza do Schema Registry (excluindo os schemas JSON antigas) e mudei o prefixo do tópico de `neon-desafio` para `neon-v2`. Defini a formatação de saída obrigatória:
*   `output.data.format`: **AVRO**
*   `output.key.format`: **AVRO**

![Connector neon-v2](prints/connectors.png)

### Fase 4: O Motor de Regras no Flink SQL

Com os tópicos AVRO criados, fui para a secção **SQL Workspaces** do Confluent e executei as seguintes queries, uma a uma:

1. **Aplicação das Chaves Primárias (Essencial para o changelog):**
```sql
ALTER TABLE `desafio-final`.`desafio-basic`.`neon-v2.payments.accounts` ADD PRIMARY KEY (account_id) NOT ENFORCED;
ALTER TABLE `desafio-final`.`desafio-basic`.`neon-v2.payments.transactions` ADD PRIMARY KEY (transaction_id) NOT ENFORCED;
```

2. **Criação da Tabela Enriquecida (Join do stream):**
```sql
CREATE TABLE `desafio-final`.`desafio-basic`.`desafio.transactions.enriched` AS
SELECT 
    t.transaction_id, 
    t.card_id, 
    t.amount, 
    a.account_id, 
    a.status AS account_status
FROM `desafio-final`.`desafio-basic`.`neon-v2.payments.transactions` t
JOIN `desafio-final`.`desafio-basic`.`neon-v2.payments.accounts` FOR SYSTEM_TIME AS OF t.`$rowtime` AS a
ON t.account_id = a.account_id;
```

3. **Configuração para Modo Append:**
```sql
ALTER TABLE `desafio-final`.`desafio-basic`.`desafio.transactions.enriched` SET ('changelog.mode' = 'append');
```

4. **A Regra Antifraude (Complex Event Processing):**
```sql
CREATE TABLE `desafio-final`.`desafio-basic`.`desafio.fraud.detected` AS
SELECT *
FROM `desafio-final`.`desafio-basic`.`desafio.transactions.enriched`
MATCH_RECOGNIZE (
    PARTITION BY card_id
    ORDER BY `$rowtime`
    MEASURES 
        FIRST(T.`$rowtime`) AS first_txn, 
        LAST(T.`$rowtime`) AS last_txn, 
        COUNT(T.transaction_id) AS txn_count
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (T{3,}?) WITHIN INTERVAL '60' SECOND
    DEFINE T AS T.amount > 0
);
```

![Workspace do Flink rodando os jobs de streaming](prints/flink_workspace.png)

### 🏆 Teste e Evidência Final

Para simular a fraude, disparei este `INSERT` **3 vezes seguidas de forma muito rápida** no meu terminal Ubuntu:

```bash
docker run --rm postgres:16-alpine psql "postgres://user:***@host/dbname?sslmode=require" -c "INSERT INTO payments.transactions (transaction_id, account_id, card_id, amount) VALUES (gen_random_uuid(), '11111111-1111-1111-1111-111111111111', '22222222-2222-2222-2222-222222222222', 50.00);"
```

O Flink detetou o padrão e disparou a mensagem de alerta para o tópico final `desafio.fraud.detected`.

![Fraude Detectada](prints/fraud.png)

---

## 💸 Análise de Custos e Limpeza

Este projeto utiliza recursos gerenciados da Confluent Cloud que são cobrados por hora de uso contínuo (Compute Pools e Connectors). 

*   **Custos da Arquitetura:** 
    *   Conector Debezium Postgres CDC: ~US$ 0.50 por hora.
    *   Flink Compute Pool: ~US$ 0.50 por hora.
    *   *Total Estimado:* ~US$ 1.00 por hora enquanto os serviços estiverem com status *Running*.
*   **Gestão de Custos:** Como o projeto utilizou o bônus promocional de US$ 400.00 da Confluent Cloud, a arquitetura foi planejada para ser temporária.
*   **Desligamento (Tear Down):** Imediatamente após a validação do alerta de fraude, acessei a interface da Confluent Cloud e executei a parada dos fluxos para não esgotar os créditos:
    1. Menu SQL Workspaces > Cliquei em `Stop` em todas as queries ativas.
    2. Menu Connectors > Deletei o conector `PostgresCdcSourceV2`.
    3. Menu Flink Management > Deletei o Compute Pool alocado.