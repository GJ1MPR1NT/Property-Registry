-- ============================================================================
-- Registry-iQ — iqid_soft_delete_row: remove a registry row that isn't real
-- Target: Registry-iQ Supabase (xhafhdaugmgdxckhdfov)
--
-- Prereq: extends property_registry / project_registry status CHECK
--         constraints to accept the new 'deleted' enum value below.
--
-- Use case: junk row that got ingested into a registry (e.g. a row in
-- property_registry that isn't actually a property at all — bad import,
-- placeholder, test data, mis-categorized building, etc.). Reviewers
-- spot these in the dedupe queue ("L and R are both garbage") and need
-- a one-click way to take them out of the active set without breaking
-- FK references from downstream tables.
--
-- Soft delete, not hard. Reasoning:
--   - Hard DELETE either fails (RESTRICT) or cascades to child rows
--     (property_unit_types, schedule_iq, install_iq, etc.). Both are
--     bad — the first blocks the reviewer, the second nukes audit.
--   - Soft delete preserves FK integrity and the row, but flags it
--     in a way that excludes it from active queries and dedupe scans.
--   - Distinguishable from the merge soft-delete via the notes marker
--     ("[DELETED ...]" vs "[MERGED ...]") and, for property/project,
--     via a new status value 'deleted' (vs 'inactive').
--
-- Side effect: any OTHER open dedupe_review pairs that reference this
-- row are auto-rejected with resolution='peer_deleted', since they
-- can no longer be resolved meaningfully (one side is gone).
-- ============================================================================

-- 1. Extend CHECK constraints so property_status / project_status can take
--    the new 'deleted' value alongside the existing operational states.
ALTER TABLE public.property_registry
  DROP CONSTRAINT IF EXISTS property_registry_property_status_check,
  ADD  CONSTRAINT property_registry_property_status_check
       CHECK (property_status = ANY (ARRAY[
         'prospect'::text,
         'pre_development'::text,
         'under_construction'::text,
         'active'::text,
         'renovation'::text,
         'turn_in_progress'::text,
         'inactive'::text,
         'competitor'::text,
         'deleted'::text
       ]));

ALTER TABLE public.project_registry
  DROP CONSTRAINT IF EXISTS project_registry_project_status_check,
  ADD  CONSTRAINT project_registry_project_status_check
       CHECK (project_status = ANY (ARRAY[
         'prospect'::text,
         'planning'::text,
         'active'::text,
         'on_hold'::text,
         'completed'::text,
         'cancelled'::text,
         'deleted'::text
       ]));

-- 2. The soft-delete function itself.

CREATE OR REPLACE FUNCTION public.iqid_soft_delete_row(
  p_entity_type public.iqid_entity_type,
  p_row_id      uuid,
  p_reviewer    text DEFAULT NULL,
  p_reason      text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_registry_table  text := p_entity_type::text || '_registry';
  v_status_column   text;
  v_use_text_status boolean;
  v_row_jsonb       jsonb;
  v_name            text;
  v_name_column     text;
  v_notes_existing  text;
  v_marker          text;
  v_fk_total        int := 0;
  v_fk_breakdown    jsonb := '[]'::jsonb;
  fk_row            record;
  v_fk_count        int;
  v_target_value    text;
  v_peer_pairs_reset int := 0;
BEGIN
  v_name_column := CASE p_entity_type
    WHEN 'property'    THEN 'property_name'
    WHEN 'project'     THEN 'project_name'
    WHEN 'vendor'      THEN 'vendor_name'
    WHEN 'stakeholder' THEN 'stakeholder_name'
    WHEN 'contact'     THEN 'display_name'
    WHEN 'facility'    THEN 'facility_name'
  END;

  v_status_column := CASE p_entity_type
    WHEN 'property' THEN 'property_status'
    WHEN 'project'  THEN 'project_status'
    ELSE 'is_active'
  END;

  v_use_text_status := p_entity_type IN ('property','project');

  EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1', v_registry_table)
    INTO v_row_jsonb USING p_row_id;

  IF v_row_jsonb IS NULL THEN
    RAISE EXCEPTION 'Row % not found in %', p_row_id, v_registry_table;
  END IF;

  v_name := v_row_jsonb->>v_name_column;

  -- Audit: count FK rows that point at this row (informational only;
  -- we don't repoint anything — the FKs continue to point at the now
  -- soft-deleted row, which is intentional).
  FOR fk_row IN
    SELECT
      conrelid::regclass::text AS source_table,
      a.attname                AS source_column,
      af.attname               AS target_column,
      conname                  AS constraint_name
    FROM pg_constraint c
    JOIN pg_attribute a  ON a.attrelid  = c.conrelid  AND a.attnum  = c.conkey[1]
    JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = c.confkey[1]
    WHERE c.contype = 'f'
      AND confrelid::regclass::text = v_registry_table
  LOOP
    EXECUTE format('SELECT (%I)::text FROM public.%I WHERE id = $1',
                   fk_row.target_column, v_registry_table)
      INTO v_target_value USING p_row_id;
    IF v_target_value IS NULL THEN CONTINUE; END IF;
    EXECUTE format('SELECT COUNT(*) FROM %s WHERE (%I)::text = $1',
                   fk_row.source_table, fk_row.source_column)
      INTO v_fk_count USING v_target_value;
    IF v_fk_count > 0 THEN
      v_fk_breakdown := v_fk_breakdown || jsonb_build_array(jsonb_build_object(
        'source_table',  fk_row.source_table,
        'source_column', fk_row.source_column,
        'row_count',     v_fk_count
      ));
      v_fk_total := v_fk_total + v_fk_count;
    END IF;
  END LOOP;

  -- Build the [DELETED ...] notes marker
  v_marker := '[DELETED ' || to_char(now(), 'YYYY-MM-DD HH24:MI') || ']'
              || CASE WHEN p_reason   IS NOT NULL THEN ' reason: ' || p_reason   ELSE '' END
              || CASE WHEN p_reviewer IS NOT NULL THEN ' by '       || p_reviewer ELSE '' END
              || ' (row marked deleted via registry-review; FKs left intact)';

  EXECUTE format('SELECT notes FROM public.%I WHERE id = $1', v_registry_table)
    INTO v_notes_existing USING p_row_id;

  -- Apply soft-delete + notes marker
  IF v_use_text_status THEN
    EXECUTE format(
      'UPDATE public.%I SET %I = ''deleted'', notes = $1 WHERE id = $2',
      v_registry_table, v_status_column
    ) USING coalesce(v_notes_existing || E'\n\n', '') || v_marker,
            p_row_id;
  ELSE
    EXECUTE format(
      'UPDATE public.%I SET %I = false, notes = $1 WHERE id = $2',
      v_registry_table, v_status_column
    ) USING coalesce(v_notes_existing || E'\n\n', '') || v_marker,
            p_row_id;
  END IF;

  -- Auto-reject any OTHER open dedupe pairs that reference this row.
  -- This keeps the queue clean — once a row is declared "not real,"
  -- every pairing involving it is moot.
  UPDATE public.registry_dedupe_review
     SET review_status  = 'rejected',
         resolution     = 'peer_deleted',
         reviewer_notes = coalesce(reviewer_notes || E'\n\n', '') ||
                          '[AUTO-REJECTED ' || to_char(now(), 'YYYY-MM-DD HH24:MI') ||
                          '] Peer row (' || p_row_id::text || ') was deleted: ' ||
                          coalesce(p_reason, '(no reason given)'),
         reviewed_by    = coalesce(p_reviewer, 'system'),
         reviewed_at    = now()
   WHERE entity_type   = p_entity_type
     AND review_status IN ('pending','auto_matched','ai_proposed','needs_review')
     AND (left_id = p_row_id OR right_id = p_row_id);

  GET DIAGNOSTICS v_peer_pairs_reset = ROW_COUNT;

  RETURN jsonb_build_object(
    'ok',                   true,
    'entity_type',          p_entity_type,
    'row_id',               p_row_id,
    'name',                 v_name,
    'iqid',                 v_row_jsonb->>'iqid',
    'status_column',        v_status_column,
    'status_set_to',        CASE WHEN v_use_text_status THEN to_jsonb('deleted'::text) ELSE to_jsonb(false) END,
    'reason',               p_reason,
    'reviewer',             p_reviewer,
    'fk_total',             v_fk_total,
    'fk_breakdown',         v_fk_breakdown,
    'fk_policy',            'left intact; FKs continue to reference this row for audit',
    'peer_pairs_rejected',  v_peer_pairs_reset,
    'deleted_at',           now()
  );
END;
$$;

COMMENT ON FUNCTION public.iqid_soft_delete_row(public.iqid_entity_type, uuid, text, text) IS
  'Marks a registry row as deleted (property/project: status=deleted; others: is_active=false) with a [DELETED] notes marker. Use when a row was ingested into a registry but isn''t actually that kind of entity. Does NOT touch FKs; child rows continue to reference the soft-deleted row for audit. Auto-rejects every other open dedupe_review pair that references this row.';
