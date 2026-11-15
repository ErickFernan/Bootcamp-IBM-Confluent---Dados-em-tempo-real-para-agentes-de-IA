-- 1. Definição das Chaves Primárias para permitir o rastreio de alterações (changelog)
ALTER TABLE `desafio-final`.`desafio-basic`.`neon-v2.payments.accounts` ADD PRIMARY KEY (account_id) NOT ENFORCED;

ALTER TABLE `desafio-final`.`desafio-basic`.`neon-v2.payments.transactions` ADD PRIMARY KEY (transaction_id) NOT ENFORCED;

-- 2. Enriquecimento dos dados de transação com o status da conta (Join contínuo no stream)
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

-- 3. Configuração do modo de inserção (Obrigatório para o processamento de padrões CEP)
ALTER TABLE `desafio-final`.`desafio-basic`.`desafio.transactions.enriched` SET ('changelog.mode' = 'append');

-- 4. Motor de Regras (CEP) para detecção de fraudes (3 transações em 60 segundos)
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