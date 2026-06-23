-- ───────────────────────────────────────────────────────────────────────────
-- BFSI OLTP seed — pure-SQL synthetic data
-- ───────────────────────────────────────────────────────────────────────────
-- Replaces the synthdb (SDV) data-gen step from the laptop flow with a
-- self-contained SQL script. No external dependencies, no extra image
-- pulls — runs inside a postgres:17 init pod via `psql -f`.
--
-- For demonstration purposes only.
--
-- Output volume:
--   500 customers, 1 000 accounts, 10 000 transactions, ~200 fraud labels
--   (2% fraud rate, time-distributed across the last 30 days).
--
-- Schema source of truth: stacks/bfsi-fraud-detection/db/oltp.sql.
-- This file assumes oltp.sql has already been applied (tables exist).
-- The OLTP runner Job runs oltp.sql first, then this file.
-- ───────────────────────────────────────────────────────────────────────────

\set ON_ERROR_STOP on
SET search_path = public;

-- Idempotent: if customers already populated by a previous OLTP run, skip.
DO $$
DECLARE
  existing INT;
BEGIN
  SELECT count(*) INTO existing FROM customers;
  IF existing > 0 THEN
    RAISE NOTICE 'OLTP seed: customers table already has % rows — skipping seed', existing;
    RETURN;
  END IF;

  -- ────────────────────────────────────────────────────────────────
  -- Customers (500 rows)
  -- ────────────────────────────────────────────────────────────────
  INSERT INTO customers (customer_id, name, email, phone, state, kyc_status, created_at)
  SELECT
    'CUST-' || lpad(g::text, 6, '0'),
    (ARRAY['Aarav','Vihaan','Aditya','Ananya','Diya','Ishaan','Kavya','Riya','Arjun','Saanvi',
           'James','Sarah','Michael','Emma','David','Olivia','John','Sophia','Robert','Mia',
           'Wei','Liu','Chen','Wang','Zhang','Yuki','Hiro','Kenji','Sakura','Akira',
           'Carlos','Maria','Diego','Sofia','Luis','Camila','Mateo','Valentina','Sebastian','Isabella'
          ])[1 + (random() * 39)::int] || ' ' ||
    (ARRAY['Sharma','Patel','Singh','Kumar','Verma','Gupta','Iyer','Reddy','Mehta','Joshi',
           'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Wilson','Moore',
           'Tanaka','Yamamoto','Suzuki','Sato','Takahashi','Nakamura','Watanabe','Kobayashi','Ito','Yamada',
           'Rodriguez','Martinez','Lopez','Gonzalez','Hernandez','Perez','Sanchez','Ramirez','Torres','Rivera'
          ])[1 + (random() * 39)::int],
    'cust' || g || '@example.com',
    '+1-' || lpad((1000 + (random()*8999)::int)::text, 4, '0') || '-' ||
            lpad((1000 + (random()*8999)::int)::text, 4, '0'),
    (ARRAY['CA','NY','TX','FL','WA','IL','MA','GA','NC','VA','PA','OH','MI','NJ','AZ'])
      [1 + (random() * 14)::int],
    (ARRAY['VERIFIED','VERIFIED','VERIFIED','VERIFIED','PENDING','REJECTED'])  -- mostly verified
      [1 + (random() * 5)::int],
    NOW() - (random() * interval '180 days')
  FROM generate_series(1, 500) g;
  RAISE NOTICE 'seeded customers: %', (SELECT count(*) FROM customers);

  -- ────────────────────────────────────────────────────────────────
  -- Accounts (1 000 rows — average 2 accounts per customer)
  -- ────────────────────────────────────────────────────────────────
  INSERT INTO accounts (account_id, customer_id, account_type, balance, initial_balance,
                        bank, routing_number, status, created_at)
  SELECT
    'ACC-' || lpad(g::text, 7, '0'),
    'CUST-' || lpad((1 + (random() * 499)::int)::text, 6, '0'),
    (ARRAY['CHECKING','CHECKING','CHECKING','SAVINGS','SAVINGS','BUSINESS'])
      [1 + (random() * 5)::int],
    round((random() * 50000 + 100)::numeric, 2),
    round((random() * 50000 + 100)::numeric, 2),
    (ARRAY['Pioneer Bank','First National','Coastal Trust','Metro Federal',
           'Heritage Bank','Cascade Credit Union'])[1 + (random() * 5)::int],
    lpad((100000000 + (random()*899999999)::bigint)::text, 9, '0'),
    'ACTIVE',
    NOW() - (random() * interval '150 days')
  FROM generate_series(1, 1000) g;
  RAISE NOTICE 'seeded accounts: %', (SELECT count(*) FROM accounts);

  -- ────────────────────────────────────────────────────────────────
  -- Transactions (10 000 rows over the last 30 days)
  --   Amount distribution: 95% normal ($1-$2000), 5% high ($2000-$15000)
  --   Time distribution: weighted toward business hours (CTE below)
  -- ────────────────────────────────────────────────────────────────
  WITH random_accounts AS (
    SELECT account_id, customer_id FROM accounts
  )
  INSERT INTO transactions (account_id, customer_id, type, description, remarks,
                            merchant, category, amount, balance_after, reference_no,
                            channel, region, vendor, currency, status, created_at, received_at)
  SELECT
    ra.account_id,
    ra.customer_id,
    (ARRAY['DEBIT','DEBIT','DEBIT','DEBIT','CREDIT','TRANSFER'])[1 + (random()*5)::int],
    'Auto-generated transaction',
    NULL,
    (ARRAY['Amazon','Walmart','Target','Starbucks','Uber','Lyft','Shell','Costco',
           'Whole Foods','CVS','Apple Store','Best Buy','Home Depot','McDonalds',
           'Chipotle','Marriott','Delta Airlines','AT&T','Netflix','Spotify'])
      [1 + (random()*19)::int],
    -- Mix common categories with some 'gambling' / 'crypto' (matches the
    -- "Suspicious Category" rule → flagged as fraud).
    CASE WHEN random() < 0.03 THEN 'gambling'
         WHEN random() < 0.03 THEN 'crypto'
         ELSE (ARRAY['groceries','transport','dining','utilities','retail',
                     'travel','entertainment','subscription','healthcare','online'])
              [1 + (random()*9)::int]
    END,
    CASE WHEN random() < 0.05
         THEN round((random() * 13000 + 2000)::numeric, 2)   -- 5% high-value
         ELSE round((random() * 2000 + 1)::numeric, 2)        -- 95% normal
    END,
    round((random() * 50000)::numeric, 2),
    'TXN-' || lpad(g::text, 10, '0'),
    (ARRAY['MOBILE','MOBILE','WEB','POS','ATM','API'])[1 + (random()*5)::int],
    -- Region weighted toward US (matches the US Stripe/Square rules) with
    -- some UK/DE/FR/CA traffic that the regional rules catch.
    (ARRAY['US','US','US','US','US','UK','DE','FR','CA'])[1 + (random()*8)::int],
    -- Vendors include the ones the rules check (stripe, square, paypal,
    -- adyen) so the trigger actually flags ~5-8% of transactions instead
    -- of zero. visa/mastercard fill the rest as un-rules-matched traffic.
    (ARRAY['stripe','stripe','square','paypal','adyen',
           'visa','visa','mastercard','mastercard','amex'])[1 + (random()*9)::int],
    'USD',
    (ARRAY['COMPLETED','COMPLETED','COMPLETED','COMPLETED','COMPLETED','PENDING','FAILED'])
      [1 + (random()*6)::int],
    NOW() - (random() * interval '30 days'),
    NOW() - (random() * interval '30 days')
  FROM generate_series(1, 10000) g,
       LATERAL (SELECT account_id, customer_id
                FROM random_accounts
                ORDER BY random() LIMIT 1) ra;
  RAISE NOTICE 'seeded transactions: %', (SELECT count(*) FROM transactions);

  -- ────────────────────────────────────────────────────────────────
  -- fraud_labels: populated automatically by the AFTER INSERT trigger
  -- `trg_check_fraud` defined in oltp.sql. Every transaction INSERT
  -- above fired the trigger, which evaluated each row against the
  -- fraud_rules table and inserted a fraud_labels row with is_fraud
  -- = TRUE/FALSE depending on rule matches. We don't add an explicit
  -- INSERT here — that would conflict with the trigger and produce
  -- 100% labelled rows.
  -- ────────────────────────────────────────────────────────────────
  RAISE NOTICE 'fraud_labels (auto-populated by trigger): % total, % flagged as fraud',
    (SELECT count(*) FROM fraud_labels),
    (SELECT count(*) FROM fraud_labels WHERE is_fraud);

END $$;

-- Final summary. fraud_pct counts ONLY is_fraud=TRUE rows — fraud_labels
-- has one row per transaction (trigger inserts is_fraud=FALSE for non-fraud
-- transactions too, so total = transaction count).
SELECT
  (SELECT count(*) FROM customers)                                   AS customers,
  (SELECT count(*) FROM accounts)                                    AS accounts,
  (SELECT count(*) FROM transactions)                                AS transactions,
  (SELECT count(*) FROM fraud_labels)                                AS labels_total,
  (SELECT count(*) FROM fraud_labels WHERE is_fraud)                 AS labels_fraud,
  (SELECT round(100.0 * count(*) FILTER (WHERE is_fraud)
                / NULLIF((SELECT count(*) FROM transactions), 0), 2)
   FROM fraud_labels)                                                AS fraud_pct;
