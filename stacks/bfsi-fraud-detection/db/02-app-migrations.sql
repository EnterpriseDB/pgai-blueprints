-- Add received_at timestamp to track when OLTP received the transaction
-- This allows measuring true TTDF: prediction_time - received_at

-- For demonstration purposes only.

-- Step 1: Add the column
ALTER TABLE transactions
ADD COLUMN IF NOT EXISTS received_at TIMESTAMP DEFAULT NOW();

-- Step 2: Backfill for existing transactions (use created_at as approximation)
-- For historical data, we assume received_at ≈ created_at
UPDATE transactions
SET received_at = created_at
WHERE received_at IS NULL;

-- Step 3: Create index for performance
CREATE INDEX IF NOT EXISTS idx_transactions_received_at
ON transactions(received_at);

-- Step 4: Set NOT NULL constraint after backfill
ALTER TABLE transactions
ALTER COLUMN received_at SET NOT NULL;

-- Verify
SELECT
    COUNT(*) as total_transactions,
    COUNT(received_at) as with_received_at,
    MIN(received_at) as earliest_received,
    MAX(received_at) as latest_received,
    NOW() as current_time
FROM transactions;

COMMENT ON COLUMN transactions.received_at IS
'Timestamp when transaction was received by OLTP system (NOW() on INSERT). Used to calculate TTDF = prediction_time - received_at';
