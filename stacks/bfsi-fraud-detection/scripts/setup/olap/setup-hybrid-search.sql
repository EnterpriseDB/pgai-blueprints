-- ============================================================================
-- BFSI Hybrid Search — VectorChord-BM25 + AIDB BERT + RRF
-- ============================================================================
--
-- For demonstration purposes only.
--
-- Three retrieval modes over transactions.description + remarks:
--   * bm25 — VectorChord-BM25 (Okapi BM25, requires vchord_bm25 + pg_tokenizer)
--   * aidb — AIDB BERT embeddings (cosine via pgvector)
--   * rrf  — Reciprocal Rank Fusion blending bm25 + aidb (1/(60+rank))
--
-- Entry point: transactions_hybrid_search(q text, k int, mode text)
-- Applied by setup_metabase.py at end of UC2 OLAP Start Service.
-- Idempotent: re-running rebuilds the AIDB pipeline + refreshes BM25 tokens.
-- ============================================================================

SET client_min_messages = WARNING;
SET search_path TO public, tokenizer_catalog, bm25_catalog;

-- ── Extensions (entrypoint already loads vchord_bm25, pg_tokenizer, aidb) ──
CREATE EXTENSION IF NOT EXISTS vchord_bm25;
CREATE EXTENSION IF NOT EXISTS pg_tokenizer;
CREATE EXTENSION IF NOT EXISTS aidb CASCADE;
CREATE EXTENSION IF NOT EXISTS vector;

-- ── BM25 keyword arm: tokens column + index ────────────────────────────────
ALTER TABLE transactions
    ADD COLUMN IF NOT EXISTS bm25_tokens bm25_catalog.bm25vector;

-- Default BERT WordPiece tokenizer.
DO $$
BEGIN
    PERFORM tokenizer_catalog.create_tokenizer('bert', $cfg$
model = "bert_base_uncased"
$cfg$);
EXCEPTION WHEN OTHERS THEN NULL; -- already exists
END $$;

-- BM25 backfill runs from Python in setup_metabase.py: batched UPDATEs with
-- explicit commits between batches to bound memory (the BERT tokenizer in
-- vchord_bm25 leaks intermediate state — a single large UPDATE OOMs pgd
-- even at 4GB). Cannot batch inside a DO block here because psycopg2's
-- multi-statement execute() rejects COMMIT in a nested DO with
-- "invalid transaction termination", even with autocommit=True.

CREATE INDEX IF NOT EXISTS idx_transactions_bm25
    ON transactions USING bm25 (bm25_tokens bm25_catalog.bm25_ops);

-- Keep bm25_tokens fresh as the Bank App inserts new rows.
CREATE OR REPLACE FUNCTION _trg_bm25_tokens_refresh() RETURNS TRIGGER AS $$
BEGIN
    NEW.bm25_tokens := tokenizer_catalog.tokenize(
        coalesce(NEW.description,'') || ' ' ||
        coalesce(NEW.remarks,'')     || ' ' ||
        coalesce(NEW.merchant,'')    || ' ' ||
        coalesce(NEW.category,''),
        'bert'
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_bm25_tokens ON transactions;
CREATE TRIGGER trg_bm25_tokens
    BEFORE INSERT OR UPDATE OF description, remarks, merchant, category
    ON transactions
    FOR EACH ROW EXECUTE FUNCTION _trg_bm25_tokens_refresh();

-- ── AIDB semantic arm: knowledge base over the same content ────────────────
DO $$
BEGIN
    PERFORM aidb.delete_pipeline('transactions_kb');
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

DROP TABLE IF EXISTS public.pipeline_transactions_kb CASCADE;

-- Source for AIDB: a generated content column on transactions.
ALTER TABLE transactions
    ADD COLUMN IF NOT EXISTS search_content TEXT
    GENERATED ALWAYS AS (
        coalesce(description,'') || ' ' ||
        coalesce(remarks,'')     || ' ' ||
        coalesce(merchant,'')    || ' ' ||
        coalesce(category,'')
    ) STORED;

-- Bounded corpus table for AIDB — populated by Python after BM25 backfill.
-- AIDB pipelines reject views as sources (relkind='v' fails its introspection
-- check), so this is a real table. Empty at create time; rows inserted from
-- transactions once BM25 tokenization succeeds, so AIDB only embeds what
-- BM25 has indexed. Without this AIDB would try to embed all 127k rows.
CREATE TABLE IF NOT EXISTS public.transactions_hybrid_corpus (
    tx_id          BIGINT PRIMARY KEY,
    search_content TEXT
);
TRUNCATE public.transactions_hybrid_corpus;

SELECT * FROM aidb.create_pipeline(
    name              => 'transactions_kb',
    source            => 'public.transactions_hybrid_corpus',
    source_key_column => 'tx_id',
    source_data_column=> 'search_content',
    step_1            => 'KnowledgeBase'::aidb.pipelinestepoperation,
    step_1_options    => aidb.knowledge_base_config('bert', 'Text'::aidb.pipelinedataformat),
    destination       => 'public.pipeline_transactions_kb',
    auto_processing   => 'Disabled'::aidb.pipelineautoprocessingmode,
    batch_size        => 100
);

-- aidb.run_pipeline() is called from Python AFTER the BM25 backfill so the
-- view has rows to embed. Calling it here would see an empty view.

-- Hybrid search modes: bm25 | aidb | rrf | reranked | fraud
DROP FUNCTION IF EXISTS transactions_hybrid_search(TEXT, INTEGER, TEXT);
CREATE OR REPLACE FUNCTION transactions_hybrid_search(
    q     TEXT,
    k     INTEGER DEFAULT 10,
    mode  TEXT    DEFAULT 'rrf'
)
RETURNS TABLE (
    tx_id        BIGINT,
    merchant     TEXT,
    category     TEXT,
    amount       DOUBLE PRECISION,
    is_fraud     BOOLEAN,
    fraud_reason TEXT,
    description  TEXT,
    found_by     TEXT,
    score        NUMERIC,
    bm25_rank    INTEGER,
    sem_rank     INTEGER,
    query_ms     INTEGER
)
SET search_path = public, bm25_catalog, tokenizer_catalog, aidb
AS $$
DECLARE
    pool      INTEGER := GREATEST(k * 4, 20);
    t_start   TIMESTAMPTZ := clock_timestamp();
    elapsed   INTEGER;
    rerank_ok BOOLEAN := FALSE;
BEGIN
    IF mode = 'reranked' THEN
        BEGIN
            RETURN QUERY
            WITH
            bm25_arm AS (
                SELECT t.tx_id AS id,
                       ROW_NUMBER() OVER (ORDER BY t.bm25_tokens <&> bm25_catalog.to_bm25query(
                           'idx_transactions_bm25'::regclass,
                           tokenizer_catalog.tokenize(q, 'bert')::bm25_catalog.bm25vector
                       ))::INTEGER AS rnk
                FROM transactions t
                WHERE t.bm25_tokens IS NOT NULL
                ORDER BY t.bm25_tokens <&> bm25_catalog.to_bm25query(
                    'idx_transactions_bm25'::regclass,
                    tokenizer_catalog.tokenize(q, 'bert')::bm25_catalog.bm25vector
                )
                LIMIT 20
            ),
            aidb_arm AS (
                SELECT (kb.source_id)::BIGINT AS id,
                       ROW_NUMBER() OVER (ORDER BY kb.value <=>
                           aidb.kb_query_encode('public.pipeline_transactions_kb', q)::vector
                       )::INTEGER AS rnk
                FROM public.pipeline_transactions_kb kb
                ORDER BY kb.value <=>
                    aidb.kb_query_encode('public.pipeline_transactions_kb', q)::vector
                LIMIT 20
            ),
            blended AS (
                SELECT
                    COALESCE(b.id, a.id) AS id,
                    b.rnk AS brnk, a.rnk AS arnk,
                    (CASE WHEN b.rnk IS NOT NULL THEN 1.0/(60+b.rnk) ELSE 0 END) +
                    (CASE WHEN a.rnk IS NOT NULL THEN 1.0/(60+a.rnk) ELSE 0 END) AS s
                FROM bm25_arm b FULL OUTER JOIN aidb_arm a ON a.id = b.id
            ),
            candidates AS (
                SELECT bl.id, bl.brnk, bl.arnk,
                       t.search_content AS doc_text
                FROM blended bl
                JOIN transactions t ON t.tx_id = bl.id
                WHERE bl.s > 0
                ORDER BY bl.s DESC
                LIMIT 20
            ),
            reranked AS (
                -- aidb.rerank_text falls back to RRF via outer EXCEPTION.
                SELECT r.idx, r.score
                FROM (SELECT array_agg(doc_text ORDER BY brnk NULLS LAST, arnk NULLS LAST) AS docs
                      FROM candidates) c,
                     LATERAL aidb.rerank_text('bge_reranker_base', q, c.docs) r
            ),
            fraud_join AS (
                SELECT DISTINCT ON (fl.tx_id) fl.tx_id, fl.is_fraud, fl.fraud_reason
                FROM fraud_labels fl WHERE fl.detection_source = 'rules'
            )
            SELECT
                t.tx_id, t.merchant::TEXT, t.category::TEXT, t.amount,
                COALESCE(fl.is_fraud, FALSE), fl.fraud_reason, t.description,
                ('Reranked')::TEXT AS found_by,
                ROUND(rr.score::NUMERIC, 6) AS score,
                c.brnk AS bm25_rank, c.arnk AS sem_rank,
                (EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000)::INTEGER AS query_ms
            FROM reranked rr
            JOIN candidates c ON c.id = (SELECT id FROM candidates ORDER BY id LIMIT 1 OFFSET rr.idx - 1)
            JOIN transactions t ON t.tx_id = c.id
            LEFT JOIN fraud_join fl ON fl.tx_id = t.tx_id
            ORDER BY rr.score DESC
            LIMIT k;
            rerank_ok := TRUE;
        EXCEPTION WHEN OTHERS THEN
            rerank_ok := FALSE;
        END;
        IF rerank_ok THEN RETURN; END IF;
        mode := 'rrf';
    END IF;

    RETURN QUERY
    WITH
    bm25_arm AS (
        SELECT t.tx_id AS id,
               ROW_NUMBER() OVER (ORDER BY t.bm25_tokens <&> bm25_catalog.to_bm25query(
                   'idx_transactions_bm25'::regclass,
                   tokenizer_catalog.tokenize(q, 'bert')::bm25_catalog.bm25vector
               ))::INTEGER AS rnk
        FROM transactions t
        WHERE t.bm25_tokens IS NOT NULL
        ORDER BY t.bm25_tokens <&> bm25_catalog.to_bm25query(
            'idx_transactions_bm25'::regclass,
            tokenizer_catalog.tokenize(q, 'bert')::bm25_catalog.bm25vector
        )
        LIMIT pool
    ),
    aidb_arm AS (
        SELECT (kb.source_id)::BIGINT AS id,
               ROW_NUMBER() OVER (ORDER BY kb.value <=>
                   aidb.kb_query_encode('public.pipeline_transactions_kb', q)::vector
               )::INTEGER AS rnk
        FROM public.pipeline_transactions_kb kb
        ORDER BY kb.value <=>
            aidb.kb_query_encode('public.pipeline_transactions_kb', q)::vector
        LIMIT pool
    ),
    blended AS (
        SELECT
            COALESCE(b.id, a.id) AS id,
            b.rnk AS brnk,
            a.rnk AS arnk,
            (CASE WHEN mode IN ('rrf','bm25','fraud') AND b.rnk IS NOT NULL
                  THEN 1.0 / (60 + b.rnk) ELSE 0 END) +
            (CASE WHEN mode IN ('rrf','aidb','fraud') AND a.rnk IS NOT NULL
                  THEN 1.0 / (60 + a.rnk) ELSE 0 END) AS s
        FROM bm25_arm b
        FULL OUTER JOIN aidb_arm a ON a.id = b.id
    ),
    fraud_join AS (
        SELECT DISTINCT ON (fl.tx_id) fl.tx_id, fl.is_fraud, fl.fraud_reason
        FROM fraud_labels fl WHERE fl.detection_source = 'rules'
    )
    SELECT
        t.tx_id,
        t.merchant::TEXT,
        t.category::TEXT,
        t.amount,
        COALESCE(fl.is_fraud, FALSE),
        fl.fraud_reason,
        t.description,
        (CASE
            WHEN bl.brnk IS NOT NULL AND bl.arnk IS NOT NULL THEN 'Both'
            WHEN bl.brnk IS NOT NULL                         THEN 'Keyword'
            WHEN bl.arnk IS NOT NULL                         THEN 'Semantic'
            ELSE                                                  ''
        END)::TEXT AS found_by,
        ROUND(bl.s::NUMERIC, 6) AS score,
        bl.brnk AS bm25_rank,
        bl.arnk AS sem_rank,
        (EXTRACT(EPOCH FROM (clock_timestamp() - t_start)) * 1000)::INTEGER AS query_ms
    FROM blended bl
    JOIN transactions t ON t.tx_id = bl.id
    LEFT JOIN fraud_join fl ON fl.tx_id = t.tx_id
    WHERE bl.s > 0
      AND (mode != 'fraud' OR COALESCE(fl.is_fraud, FALSE) = TRUE)
    ORDER BY bl.s DESC
    LIMIT k;
END;
$$ LANGUAGE plpgsql;

-- Summary
SELECT
    'Hybrid search ready' AS status,
    (SELECT COUNT(*) FROM transactions WHERE bm25_tokens IS NOT NULL) AS bm25_tokenized,
    (SELECT COUNT(*) FROM public.pipeline_transactions_kb) AS bert_embedded;
