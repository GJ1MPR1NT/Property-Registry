-- ============================================================================
-- Registry-iQ — iqid_apply_merge: execute a merge decision (mutating)
-- Target: Registry-iQ Supabase (xhafhdaugmgdxckhdfov)
--
-- Companion to iqid_dry_run_merge (#7a). Same FK walk and plan, but this
-- function actually performs the mutations in a single plpgsql transaction:
--
--   1. Mint survivor.iqid if missing (via iqid_mint).
--   2. Merge external_ids: keys on loser that survivor doesn't have get
--      copied; conflicts keep survivor's value (loser's recorded in the
--      return payload for audit).
--   3. For every FK pointing at the registry table, UPDATE source rows
--      from the loser's target value to the survivor's target value.
--      Self-referential FKs handled the same way (project parent, etc.).
--   4. Insert registry_alias (loser's display name → survivor.iqid).
--      ON CONFLICT DO NOTHING — already-known aliases stay put.
--   5. Soft-delete the loser:
--        property:    property_status = 'inactive'
--        project:     project_status  = 'inactive'
--        other:       is_active       = false
--      Plus append a marker to notes so the audit trail is human-readable.
--
-- Also adds an applied_at timestamp column on registry_dedupe_review so
-- the review UI can distinguish "decided but not yet applied" from "applied".
-- ============================================================================

-- Add applied_at tracking column on the dedupe review
ALTER TABLE public.registry_dedupe_review
  ADD COLUMN IF NOT EXISTS applied_at        timestamptz,
  ADD COLUMN IF NOT EXISTS apply_report      jsonb,
  ADD COLUMN IF NOT EXISTS apply_error       text;

CREATE INDEX IF NOT EXISTS registry_dedupe_review_pending_apply_idx
  ON public.registry_dedupe_review (entity_type, applied_at)
  WHERE review_status = 'merged' AND applied_at IS NULL;

-- The apply function
CREATE OR REPLACE FUNCTION public.iqid_apply_merge(
  p_entity_type public.iqid_entity_type,
  p_loser_id    uuid,
  p_survivor_id uuid,
  p_reviewer    text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  v_registry_table   text := p_entity_type::text || '_registry';
  v_name_column      text;
  v_status_column    text;
  v_status_to_set    text;
  v_loser_row        jsonb;
  v_survivor_row     jsonb;
  v_loser_ext        jsonb;
  v_survivor_ext     jsonb;
  v_merged_ext       jsonb;
  v_survivor_iqid    text;
  v_loser_name       text;
  v_actions          jsonb := '[]'::jsonb;
  v_total_rows       int   := 0;
  v_minted_iqid      boolean := false;
  fk_row             record;
  v_loser_fk_value   text;
  v_survivor_fk_value text;
  v_updated          int;
  v_notes_existing   text;
  v_warnings         text[] := ARRAY[]::text[];
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
    WHEN 'property'    THEN 'property_status'
    WHEN 'project'     THEN 'project_status'
    ELSE 'is_active'
  END;

  IF p_loser_id = p_survivor_id THEN
    RAISE EXCEPTION 'Loser and survivor cannot be the same';
  END IF;

  -- Snapshot both rows
  EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1', v_registry_table)
    INTO v_loser_row USING p_loser_id;
  EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1', v_registry_table)
    INTO v_survivor_row USING p_survivor_id;

  IF v_loser_row    IS NULL THEN RAISE EXCEPTION 'Loser % not found in %',    p_loser_id,    v_registry_table; END IF;
  IF v_survivor_row IS NULL THEN RAISE EXCEPTION 'Survivor % not found in %', p_survivor_id, v_registry_table; END IF;

  v_loser_name := v_loser_row->>v_name_column;

  -- 1. Mint survivor iqid if missing
  v_survivor_iqid := v_survivor_row->>'iqid';
  IF v_survivor_iqid IS NULL THEN
    SELECT public.iqid_mint(p_entity_type) INTO v_survivor_iqid;
    EXECUTE format('UPDATE public.%I SET iqid = $1 WHERE id = $2', v_registry_table)
      USING v_survivor_iqid, p_survivor_id;
    v_minted_iqid := true;
  END IF;

  -- 2. Merge external_ids — additive only, no overwrite
  v_loser_ext    := coalesce(v_loser_row->'external_ids',    '{}'::jsonb);
  v_survivor_ext := coalesce(v_survivor_row->'external_ids', '{}'::jsonb);
  v_merged_ext := v_survivor_ext;
  -- For each loser key not present on survivor, add it.
  SELECT v_merged_ext || jsonb_object_agg(l.key, l.value)
    INTO v_merged_ext
  FROM jsonb_each(v_loser_ext) l
  WHERE NOT (v_survivor_ext ? l.key);

  IF v_merged_ext IS NULL THEN v_merged_ext := v_survivor_ext; END IF;

  IF v_merged_ext <> v_survivor_ext THEN
    EXECUTE format('UPDATE public.%I SET external_ids = $1 WHERE id = $2', v_registry_table)
      USING v_merged_ext, p_survivor_id;
  END IF;

  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind',          'external_ids_merge',
    'keys_added',    (SELECT coalesce(jsonb_agg(l.key), '[]'::jsonb)
                       FROM jsonb_each(v_loser_ext) l
                       WHERE NOT (v_survivor_ext ? l.key)),
    'conflicts',     (SELECT coalesce(jsonb_agg(jsonb_build_object(
                                'key', l.key, 'loser', l.value, 'survivor', s.value
                              )), '[]'::jsonb)
                       FROM jsonb_each(v_loser_ext) l
                       JOIN jsonb_each(v_survivor_ext) s ON l.key = s.key
                       WHERE l.value <> s.value)
  ));

  -- 3. FK repoints — walk every FK pointing at the registry table
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
      INTO v_loser_fk_value USING p_loser_id;
    EXECUTE format('SELECT (%I)::text FROM public.%I WHERE id = $1',
                   fk_row.target_column, v_registry_table)
      INTO v_survivor_fk_value USING p_survivor_id;

    IF v_loser_fk_value IS NULL OR v_survivor_fk_value IS NULL THEN
      CONTINUE;
    END IF;

    EXECUTE format('UPDATE %s SET %I = $1 WHERE (%I)::text = $2',
                   fk_row.source_table, fk_row.source_column, fk_row.source_column)
      USING v_survivor_fk_value, v_loser_fk_value;
    GET DIAGNOSTICS v_updated = ROW_COUNT;

    IF v_updated > 0 THEN
      v_actions := v_actions || jsonb_build_array(jsonb_build_object(
        'kind',           'fk_repoint',
        'source_table',   fk_row.source_table,
        'source_column',  fk_row.source_column,
        'target_column',  fk_row.target_column,
        'constraint_name', fk_row.constraint_name,
        'rows_updated',   v_updated
      ));
      v_total_rows := v_total_rows + v_updated;
    END IF;
  END LOOP;

  -- 4. Insert alias (loser name → survivor iqid)
  IF v_loser_name IS NOT NULL AND v_loser_name <> '' THEN
    BEGIN
      INSERT INTO public.registry_alias (entity_type, iqid, registry_id, alias_name, source, notes)
      VALUES (p_entity_type, v_survivor_iqid, p_survivor_id, v_loser_name,
              'iqid_apply_merge',
              'Merged loser ' || p_loser_id::text || ' (registry row soft-deleted)');
      v_actions := v_actions || jsonb_build_array(jsonb_build_object(
        'kind', 'alias_insert', 'alias_name', v_loser_name, 'iqid', v_survivor_iqid
      ));
    EXCEPTION WHEN unique_violation THEN
      -- Already-known alias; that's fine.
      v_actions := v_actions || jsonb_build_array(jsonb_build_object(
        'kind', 'alias_insert_skipped', 'alias_name', v_loser_name,
        'reason', 'already-known alias'
      ));
    END;
  END IF;

  -- 5. Soft delete the loser
  v_status_to_set := CASE p_entity_type
    WHEN 'property' THEN 'inactive'
    WHEN 'project'  THEN 'inactive'
    ELSE NULL  -- boolean false handled below
  END;

  -- Append a merge marker to notes if column exists (all 6 registries have notes)
  EXECUTE format('SELECT notes FROM public.%I WHERE id = $1', v_registry_table)
    INTO v_notes_existing USING p_loser_id;

  IF p_entity_type IN ('property','project') THEN
    EXECUTE format(
      'UPDATE public.%I SET %I = $1, notes = $2 WHERE id = $3',
      v_registry_table, v_status_column
    ) USING v_status_to_set,
            coalesce(v_notes_existing || E'\n\n', '') ||
            '[MERGED ' || to_char(now(), 'YYYY-MM-DD HH24:MI') ||
            '] Merged into ' || p_survivor_id::text ||
            ' (iqid=' || v_survivor_iqid || ')' ||
            CASE WHEN p_reviewer IS NOT NULL THEN ' by ' || p_reviewer ELSE '' END,
            p_loser_id;
  ELSE
    EXECUTE format(
      'UPDATE public.%I SET %I = false, notes = $1 WHERE id = $2',
      v_registry_table, v_status_column
    ) USING coalesce(v_notes_existing || E'\n\n', '') ||
            '[MERGED ' || to_char(now(), 'YYYY-MM-DD HH24:MI') ||
            '] Merged into ' || p_survivor_id::text ||
            ' (iqid=' || v_survivor_iqid || ')' ||
            CASE WHEN p_reviewer IS NOT NULL THEN ' by ' || p_reviewer ELSE '' END,
            p_loser_id;
  END IF;

  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind', 'soft_delete', 'status_column', v_status_column,
    'value', CASE WHEN v_status_to_set IS NOT NULL THEN to_jsonb(v_status_to_set)
                  ELSE to_jsonb(false) END
  ));

  -- Warnings for the audit log
  IF v_minted_iqid THEN
    v_warnings := v_warnings || ARRAY['minted survivor iqid: ' || v_survivor_iqid];
  END IF;

  RETURN jsonb_build_object(
    'ok',                  true,
    'entity_type',         p_entity_type,
    'loser_id',            p_loser_id,
    'survivor_id',         p_survivor_id,
    'survivor_iqid',       v_survivor_iqid,
    'minted_iqid',         v_minted_iqid,
    'actions',             v_actions,
    'total_rows_affected', v_total_rows,
    'warnings',            to_jsonb(v_warnings),
    'applied_at',          now()
  );
END;
$$;

COMMENT ON FUNCTION public.iqid_apply_merge(public.iqid_entity_type, uuid, uuid, text) IS
  'Executes a merge decision atomically — mints survivor iqid if needed, merges external_ids (additive), repoints every FK to the survivor, inserts the loser name as an alias, soft-deletes the loser. Returns a jsonb report with actual rows affected. Companion: iqid_dry_run_merge.';
