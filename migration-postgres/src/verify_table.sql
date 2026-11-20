-- Exact v1 shape on PostgreSQL 17. Deliberately reject extensions (including
-- extra indexes/constraints/columns), rather than guessing that they are safe.
SELECT COALESCE(
    c.relkind = 'r' AND c.relpersistence = 'p'
    AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
    AND NOT c.relispartition AND NOT c.relhasrules
    AND c.reloptions IS NULL
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid)
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid)
    AND (
        SELECT pg_catalog.string_agg(a.attname || ':' || t.typname || ':' || a.attnotnull::text, '|' ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = c.oid AND a.attnum > 0
    ) = $4
    AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE a.attrelid = c.oid AND a.attnum > 0
          AND (a.attisdropped OR a.atthasdef OR a.attidentity <> '' OR a.attgenerated <> ''
               OR a.attcollation <> CASE WHEN t.typname = 'text'
                    THEN 'pg_catalog."C"'::pg_catalog.regcollation::oid ELSE t.typcollation END
               OR a.atttypmod <> -1
               OR a.attfdwoptions IS NOT NULL
               OR n.nspname <> 'pg_catalog')
    )
    AND (
        SELECT pg_catalog.string_agg(pg_catalog.pg_get_constraintdef(k.oid), '|' ORDER BY pg_catalog.pg_get_constraintdef(k.oid) COLLATE "C")
        FROM pg_catalog.pg_constraint k WHERE k.conrelid = c.oid
    ) = $5
    AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint k WHERE k.conrelid = c.oid
          AND (NOT k.convalidated OR k.condeferrable OR k.condeferred OR NOT k.conislocal OR k.coninhcount <> 0)
    )
    AND (SELECT count(*) FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid) = 1
    AND EXISTS (
        SELECT 1 FROM pg_catalog.pg_index i WHERE i.indrelid = c.oid
          AND i.indisprimary AND i.indisvalid AND i.indisready AND i.indislive
    ),
    false
) AS valid,
pg_catalog.obj_description(c.oid, 'pg_class') IS NOT NULL AS marked,
COALESCE(pg_catalog.obj_description(c.oid, 'pg_class') COLLATE "C" = $3, false) AS owned
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2
