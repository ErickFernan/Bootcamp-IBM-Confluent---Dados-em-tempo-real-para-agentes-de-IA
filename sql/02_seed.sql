-- sql/02_seed.sql
-- Inserindo uma conta e um cartão fictícios (UUIDs fixos para facilitar nossos testes depois)
INSERT INTO payments.accounts (account_id, status) 
VALUES ('11111111-1111-1111-1111-111111111111', 'ACTIVE');

INSERT INTO payments.cards (card_id, account_id) 
VALUES ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111');