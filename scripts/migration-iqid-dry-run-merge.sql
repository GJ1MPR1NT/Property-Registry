-- ============================================================================
-- Registry-iQ — iqid_dry_run_merge: report what an apply-merge would change
-- Target: Registry-iQ Supabase (xhafhdaugmgdxckhdfov)
--
-- Returns a jsonb plan showing every FK repoint, external_ids merge,
-- soft-delete, and alias insert that the apply worker (#7b) would execute
-- for a given (entity_type, loser_id, survivor_id) merge decision.
--
-- Designed for runtime FK discovery via pg_constraint — adding new tables
-- with FKs to the entity registries automatically lights up here, no code
-- change. Handles non-id target columns too (e.g. facility_registry's
-- facility_code FKs from outbound_legs / pull_requests).
--
-- Used by:
--   - dale-chat /api/registry-review/[id]/apply-preview (planned in #7b)
--   - admin sweep button for auto-merge band
--   - manual smoke-testing of resolved dedupe decisions
-- ============================================================================

CREATE OR REPLACE FUNCTION public.iqid_dry_run_merge(
  p_entity_type public.iqid_entity_type,
  p_loser_id    uuid,
  p_survivor_id uuid
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_registry_table text := p_entity_type::text || '_registry';
  v_name_column    text;
  v_status_column  text;
  v_loser_row      jsonb;
  v_survivor_row   jsonb;
  v_actions        jsonb := '[]'::jsonb;
  v_total_rows     int   := 0;
  v_warnings       text[] := ARRAY[]::text[];
  fk_row           record;
  v_loser_fk_value text;
  v_count          int;
  v_loser_ext      jsonb;
  v_survivor_ext   jsonb;
  v_conflicts      jsonb;
BEGIN
  -- Name + status column per entity type
  v_name_column := CASE p_entity_type
    WHEN 'property'    THEN 'property_name'
    WHEN 'project'     THEN 'project_name'
    WHEN 'vendor'      THEN 'vendor_name'
    WHEN 'stakeholder' THEN 'stakeholder_name'
    WHEN 'contact'     THEN 'display_name'
    WHEN 'facility'    THEN 'facility_name'
  END;
  v_status_column := CASE p_entity_type
    WHEN 'property'    THEN 'property_status'
    WHEN 'project'     THEN 'project_status'
    WHEN 'vendor'      THEN 'is_active'
    WHEN 'stakeholder' THEN 'is_active'
    WHEN 'contact'     THEN 'is_active'
    WHEN 'facility'    THEN 'is_active'
  END;

  IF p_loser_id = p_survivor_id THEN
    RAISE EXCEPTION 'Loser and survivor cannot be the same';
  END IF;

  EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1', v_registry_table)
    INTO v_loser_row USING p_loser_id;
  EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1', v_registry_table)
    INTO v_survivor_row USING p_survivor_id;

  IF v_loser_row IS NULL THEN
    RAISE EXCEPTION 'Loser % not found in %', p_loser_id, v_registry_table;
  END IF;
  IF v_survivor_row IS NULL THEN
    RAISE EXCEPTION 'Survivor % not found in %', p_survivor_id, v_registry_table;
  END IF;

  -- Walk every FK that points at the loser's registry table
  FOR fk_row IN
    SELECT
      conrelid::regclass::text AS source_table,
      a.attname                AS source_column,
      af.attname               AS target_column,
      conname                  AS constraint_name,
      CASE confdeltype WHEN 'a' THEN 'NO ACTION' WHEN 'r' THEN 'RESTRICT'
                       WHEN 'c' THEN 'CASCADE'   WHEN 'n' THEN 'SET NULL'
                       WHEN 'd' THEN 'SET DEFAULT' END AS on_delete
    FROM pg_constraint c
    JOIN pg_attribute a  ON a.attrelid  = c.conrelid  AND a.attnum  = c.conkey[1]
    JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = c.confkey[1]
    WHERE c.contype = 'f'
      AND confrelid::regclass::text = v_registry_table
  LOOP
    -- Look up the loser's value of the FK target column (usually id, sometimes
    -- facility_code etc.)
    EXECUTE format('SELECT (%I)::text FROM public.%I WHERE id = $1',
                   fk_row.target_column, v_registry_table)
      INTO v_loser_fk_value USING p_loser_id;

    IF v_loser_fk_value IS NULL THEN
      CONTINUE;
    END IF;

    -- Count source rows that reference the loser via this FK
    EXECUTE format('SELECT COUNT(*) FROM %s WHERE (%I)::text = $1',
                   fk_row.source_table, fk_row.source_column)
      INTO v_count USING v_loser_fk_value;

    IF v_count > 0 THEN
      v_actions := v_actions || jsonb_build_array(jsonb_build_object(
        'kind',           'fk_repoint',
        'source_table',   fk_row.source_table,
        'source_column',  fk_row.source_column,
        'target_column',  fk_row.target_column,
        'constraint_name', fk_row.constraint_name,
        'on_delete',      fk_row.on_delete,
        'row_count',      v_count,
        'self_referential', fk_row.source_table = v_registry_table
      ));
      v_total_rows := v_total_rows + v_count;
    END IF;
  END LOOP;

  -- External_ids diff
  v_loser_ext    := coalesce(v_loser_row->'external_ids',    '{}'::jsonb);
  v_survivor_ext := coalesce(v_survivor_row->'external_ids', '{}'::jsonb);

  v_conflicts := (
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'key', l.key, 'loser', l.value, 'survivor', s.value
    )), '[]'::jsonb)
    FROM jsonb_each(v_loser_ext) l
    JOIN jsonb_each(v_survivor_ext) s ON l.key = s.key
    WHERE l.value <> s.value
  );

  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind',         'external_ids_merge',
    'loser_keys',   (SELECT jsonb_agg(k) FROM jsonb_object_keys(v_loser_ext) AS k),
    'survivor_keys',(SELECT jsonb_agg(k) FROM jsonb_object_keys(v_survivor_ext) AS k),
    'conflicts',    v_conflicts,
    'merge_policy', 'loser keys not on survivor get copied; conflicts keep survivor value (loser conflict recorded)'
  ));

  -- Soft delete action descriptor
  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind',          'soft_delete',
    'status_column', v_status_column,
    'method',        CASE p_entity_type
                       WHEN 'property' THEN 'property_status := inactive'
                       WHEN 'project'  THEN 'project_status := inactive'
                       ELSE 'is_active := false'
                     END,
    'target_id',     p_loser_id
  ));

  -- Alias insert descriptor (loser's name becomes an alias of the survivor)
  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind',       'alias_insert',
    'alias_name', v_loser_row->>v_name_column,
    'iqid',       v_survivor_row->>'iqid'
  ));

  -- Warnings
  IF (v_survivor_row->>'iqid') IS NULL THEN
    v_warnings := v_warnings || ARRAY['survivor has no iqid; apply worker will mint one before alias insert'];
  END IF;
  IF (v_loser_row->>'iqid') IS NOT NULL THEN
    v_warnings := v_warnings || ARRAY['loser already has iqid ' || (v_loser_row->>'iqid') ||
                                       '; will be orphaned (kept on the soft-deleted row for audit)'];
  END IF;

  RETURN jsonb_build_object(
    'entity_type', p_entity_type,
    'loser', jsonb_build_object(
      'id',           p_loser_id,
      'iqid',         v_loser_row->>'iqid',
      'name',         v_loser_row->>v_name_column,
      'status',       v_loser_row->>v_status_column,
      'external_ids', v_loser_ext
    ),
    'survivor', jsonb_build_object(
      'id',           p_survivor_id,
      'iqid',         v_survivor_row->>'iqid',
      'name',         v_survivor_row->>v_name_column,
      'status',       v_survivor_row->>v_status_column,
      'external_ids', v_survivor_ext
    ),
    'actions',             v_actions,
    'total_rows_affected', v_total_rows,
    'warnings',            to_jsonb(v_warnings)
  );
END;
$$;

COMMENT ON FUNCTION public.iqid_dry_run_merge(public.iqid_entity_type, uuid, uuid) IS
  'Returns a jsonb plan describing what an apply-merge would change — FK repoints (with counts), external_ids merge with conflicts, soft-delete action, and alias insert. No mutations. Used by the registry-review apply preview and admin sweep.';
