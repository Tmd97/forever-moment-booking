ALTER TABLE inbound_outbox
    ADD COLUMN IF NOT EXISTS producer VARCHAR(100),
    ADD COLUMN IF NOT EXISTS event_id VARCHAR(100);

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT constraint_definition.conname
        FROM pg_constraint constraint_definition
        JOIN pg_class table_definition
          ON table_definition.oid = constraint_definition.conrelid
        JOIN pg_namespace schema_definition
          ON schema_definition.oid = table_definition.relnamespace
        WHERE table_definition.relname = 'inbound_outbox'
          AND schema_definition.nspname = current_schema()
          AND constraint_definition.contype = 'u'
          AND pg_get_constraintdef(constraint_definition.oid)
                = 'UNIQUE (booking_ref_id)'
    LOOP
        EXECUTE format(
            'ALTER TABLE inbound_outbox DROP CONSTRAINT %I',
            constraint_name);
    END LOOP;
END $$;

DROP INDEX IF EXISTS idx_inbound_outbox_ref;

CREATE INDEX IF NOT EXISTS idx_inbound_outbox_ref
    ON inbound_outbox (booking_ref_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_inbound_outbox_producer_event
    ON inbound_outbox (producer, event_id);
