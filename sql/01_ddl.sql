CREATE SCHEMA IF NOT EXISTS payments;

-- Criação da tabela de contas
CREATE TABLE payments.accounts (
    account_id UUID PRIMARY KEY,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
-- Replica Identity FULL garante que o CDC mande a linha inteira no Kafka
ALTER TABLE payments.accounts REPLICA IDENTITY FULL;

-- Criação da tabela de cartões
CREATE TABLE payments.cards (
    card_id UUID PRIMARY KEY,
    account_id UUID REFERENCES payments.accounts(account_id)
);
ALTER TABLE payments.cards REPLICA IDENTITY FULL;

-- Criação da tabela de transações (a que vai gerar os alertas)
CREATE TABLE payments.transactions (
    transaction_id UUID PRIMARY KEY,
    account_id UUID REFERENCES payments.accounts(account_id),
    card_id UUID REFERENCES payments.cards(card_id),
    merchant_id UUID,
    channel VARCHAR(50),
    amount DECIMAL(10,2),
    mcc VARCHAR(10),
    uf_transacao VARCHAR(2),
    created_at TIMESTAMP DEFAULT NOW()
);
ALTER TABLE payments.transactions REPLICA IDENTITY FULL;
