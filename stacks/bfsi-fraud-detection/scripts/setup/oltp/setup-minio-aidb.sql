-- ==============================================================================
-- AIDB + MinIO Integration for Core Banking Fraud Audit (AIDB 7.0+)
-- ==============================================================================
-- For demonstration purposes only.
--
-- Working flow: MinIO → PGFS Volume → ChunkText Pipeline → BERT Knowledge Base
-- Uses AIDB 7.0 API (create_pipeline with KnowledgeBase step)
-- ==============================================================================

\echo '════════════════════════════════════════════════════════'
\echo '  AIDB + MinIO Semantic Search Setup (AIDB 7.0)'
\echo '════════════════════════════════════════════════════════'
\echo ''

-- ============================================================================
-- Step 1: Create PGFS Storage Location for MinIO
-- ============================================================================
\echo '📦 Step 1: Creating PGFS storage location for MinIO...'

DO $$
BEGIN
    PERFORM pgfs.delete_storage_location('minio_fraud_rules');
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

SELECT pgfs.create_storage_location(
    name := 'minio_fraud_rules',
    url := 's3://fraud-rules',
    options := json_build_object(
        'endpoint', 'http://minio:9000',
        'region', 'us-east-1',
        'path_style', 'true',
        'allow_http', 'true',
        'skip_signature', 'false',
        'use_instance_credentials', 'false'
    ),
    credentials := json_build_object(
        'aws_access_key_id', 'minioadmin',
        'aws_secret_access_key', 'minioadmin123'
    )
);

\echo '✓ PGFS storage location created: minio_fraud_rules'
\echo ''

-- ============================================================================
-- Step 2: Create AIDB Volume from MinIO
-- ============================================================================
\echo '📁 Step 2: Creating AIDB volume from MinIO storage...'

DO $$
BEGIN
    PERFORM aidb.delete_volume('fraud_rules_volume');
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

SELECT aidb.create_volume(
    name := 'fraud_rules_volume',
    server_name := 'minio_fraud_rules',
    path := '/',
    data_format := 'Text'
);

\echo '✓ Volume created: fraud_rules_volume'
\echo ''

\echo '📋 Volume Contents:'
SELECT file_name, size, last_modified
FROM aidb.list_volume_content('fraud_rules_volume')
ORDER BY file_name;

\echo ''

-- ============================================================================
-- Step 3: Create ChunkText Pipeline from MinIO Volume
-- ============================================================================
\echo '⚙️  Step 3: Creating ChunkText pipeline from MinIO...'

DO $$
BEGIN
    PERFORM aidb.delete_pipeline('fraud_rules_chunk_pipeline');
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

DROP TABLE IF EXISTS public.fraud_rule_chunks CASCADE;

SELECT * FROM aidb.create_pipeline(
    name := 'fraud_rules_chunk_pipeline',
    source := 'public.fraud_rules_volume',
    step_1 := 'ChunkText'::aidb.pipelinestepoperation,
    step_1_options := jsonb_build_object(
        'chunk_size', 1000,
        'chunk_overlap', 200
    ),
    destination := 'public.fraud_rule_chunks',
    auto_processing := 'Disabled'::aidb.pipelineautoprocessingmode,
    batch_size := 10
);

\echo '✓ Pipeline created: fraud_rules_chunk_pipeline'
\echo ''

\echo '▶️  Running ChunkText pipeline...'
SELECT aidb.run_pipeline('fraud_rules_chunk_pipeline');

\echo ''
\echo '📊 Chunks Generated:'
SELECT COUNT(*) as total_chunks FROM public.fraud_rule_chunks;

\echo ''

-- ============================================================================
-- Step 4: Create INT-based chunks table (AIDB 7.0 compatibility)
-- ============================================================================
\echo '🔧 Step 4: Creating INT-based chunks table for AIDB compatibility...'

DROP TABLE IF EXISTS fraud_rule_chunks_int CASCADE;

CREATE TABLE fraud_rule_chunks_int AS
SELECT
    ROW_NUMBER() OVER () as id,
    source_id,
    value
FROM fraud_rule_chunks;

ALTER TABLE fraud_rule_chunks_int ALTER COLUMN id TYPE INTEGER;
ALTER TABLE fraud_rule_chunks_int ADD PRIMARY KEY (id);

\echo '✓ Created fraud_rule_chunks_int table'
SELECT COUNT(*) as chunks FROM fraud_rule_chunks_int;
\echo ''

-- ============================================================================
-- Step 5: Create BERT Knowledge Base Pipeline (AIDB 7.0 API)
-- ============================================================================
\echo '🤖 Step 5: Creating BERT embedding pipeline...'

DO $$
BEGIN
    PERFORM aidb.delete_pipeline('fraud_bert_kb');
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

DROP TABLE IF EXISTS public.fraud_rule_embeddings CASCADE;

SELECT * FROM aidb.create_pipeline(
    name => 'fraud_bert_kb',
    source => 'public.fraud_rule_chunks_int',
    source_key_column => 'id',
    source_data_column => 'value',
    step_1 => 'KnowledgeBase'::aidb.pipelinestepoperation,
    step_1_options => aidb.knowledge_base_config('bert', 'Text'::aidb.pipelinedataformat),
    destination => 'public.fraud_rule_embeddings',
    auto_processing => 'Disabled'::aidb.pipelineautoprocessingmode,
    batch_size => 10
);

\echo '✓ Knowledge base pipeline created: fraud_bert_kb'
\echo ''

\echo '⚙️  Generating BERT embeddings (this may take 1-2 minutes)...'
SELECT aidb.run_pipeline('fraud_bert_kb');

\echo ''
\echo '✓ Embeddings generated!'

-- ============================================================================
-- Step 6: Create Semantic Search Function
-- ============================================================================
\echo '🔍 Step 6: Creating semantic search function...'

CREATE OR REPLACE FUNCTION search_fraud_rules_semantic(
    query_text TEXT,
    top_k INTEGER DEFAULT 5
)
RETURNS TABLE (
    chunk_id INTEGER,
    source_doc TEXT,
    chunk_text TEXT,
    similarity FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.id::INTEGER as chunk_id,
        c.source_id as source_doc,
        r.value as chunk_text,
        (1 - r.distance)::FLOAT as similarity
    FROM aidb.retrieve_text('public.fraud_rule_embeddings', query_text, top_k) r
    JOIN public.fraud_rule_chunks_int c ON c.id::text = r.key;
END;
$$ LANGUAGE plpgsql;

\echo '✓ Search function created: search_fraud_rules_semantic(text, int)'
\echo ''

-- ============================================================================
-- Verification
-- ============================================================================
\echo '═══════════════════════════════════════════════════════'
\echo '  Verification Results'
\echo '═══════════════════════════════════════════════════════'
\echo ''

\echo 'Chunks:'
SELECT COUNT(*) as total_chunks FROM public.fraud_rule_chunks_int;

\echo ''
\echo 'Embeddings:'
SELECT
    COUNT(*) as total_embeddings,
    MIN(vector_dims(value)) as dims
FROM public.fraud_rule_embeddings;

\echo ''
\echo 'Pipeline Status:'
SELECT
    pipeline,
    "Status" as status,
    "count(source records)" as source_records,
    "count(destination records)" as dest_records
FROM aidb.pipeline_metrics
WHERE pipeline IN ('fraud_rules_chunk_pipeline', 'fraud_bert_kb');

\echo ''

-- ============================================================================
-- Test Search
-- ============================================================================
\echo '═══════════════════════════════════════════════════════'
\echo '  Test: Semantic Search for "North America 2024"'
\echo '═══════════════════════════════════════════════════════'
\echo ''

SELECT
    source_doc,
    ROUND(similarity::numeric, 3) as similarity,
    LEFT(chunk_text, 100) || '...' as preview
FROM search_fraud_rules_semantic('North America 2024 fraud rules', 5);

\echo ''
\echo '═══════════════════════════════════════════════════════'
\echo '  ✅ AIDB Semantic Search Setup Complete!'
\echo '═══════════════════════════════════════════════════════'
\echo ''
\echo 'What was configured:'
\echo '  • PGFS Storage: minio_fraud_rules → s3://fraud-rules'
\echo '  • AIDB Volume: fraud_rules_volume (15 documents)'
\echo '  • ChunkText Pipeline: fraud_rules_chunk_pipeline'
\echo '  • BERT Knowledge Base: fraud_bert_kb (384-dim embeddings)'
\echo '  • Search Function: search_fraud_rules_semantic(text, int)'
\echo ''
\echo 'Usage:'
\echo '  SELECT * FROM search_fraud_rules_semantic(''your query'', 5);'
\echo ''
