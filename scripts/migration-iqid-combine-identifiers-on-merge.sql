-- ============================================================================
-- Registry-iQ — combine identifiers on merge (don't discard loser's refs)
-- Target: Registry-iQ Supabase (xhafhdaugmgdxckhdfov)
--
-- Extends iqid_dry_run_merge + iqid_apply_merge (post-coalesce migration):
--
--   1. external_ids — on key collision with different values, survivor keeps
--      the primary value at the key; BOTH values are preserved under
--      external_ids.merged_refs[key] (jsonb array). No silent overwrite.
--
--   2. Scalar identifier columns — when BOTH sides have a different non-null
--      value, survivor column stays primary; loser's value is appended to an
--      external_ids alternate_* array:
--        project: project_id, order_number, d365_opportunity_code,
--                 legacy_access_project_id
--        property: property_code
--
-- Keys-only-on-loser behavior unchanged (additive). Coalesce-NULL behavior
-- unchanged (loser fills NULL survivor columns).
-- ============================================================================

-- Append a jsonb value to external_ids.<array_key> if not already present.
CREATE OR REPLACE FUNCTION public.iqid_ext_append_unique(
  p_ext       jsonb,
  p_array_key text,
  p_value     jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_ext   jsonb := coalesce(p_ext, '{}'::jsonb);
  v_arr   jsonb := coalesce(v_ext->p_array_key, '[]'::jsonb);
BEGIN
  IF p_value IS NULL OR jsonb_typeof(p_value) = 'null' THEN
    RETURN v_ext;
  END IF;
  IF jsonb_typeof(v_arr) <> 'array' THEN
    v_arr := '[]'::jsonb;
  END IF;
  IF v_arr @> jsonb_build_array(p_value) THEN
    RETURN v_ext;
  END IF;
  RETURN v_ext || jsonb_build_object(p_array_key, v_arr || jsonb_build_array(p_value));
END;
$$;

-- Merge two external_ids objects; conflicts combine into merged_refs[key][].
CREATE OR REPLACE FUNCTION public.iqid_merge_external_ids(
  p_survivor jsonb,
  p_loser    jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_result       jsonb := coalesce(p_survivor, '{}'::jsonb);
  v_merged_refs  jsonb := coalesce(v_result->'merged_refs', '{}'::jsonb);
  v_keys_added   jsonb := '[]'::jsonb;
  v_combined     jsonb := '[]'::jsonb;
  lr             record;
  v_existing_ref jsonb;
  v_new_ref      jsonb;
  v_meta_keys    text[] := ARRAY[
    'merged_refs',
    'alternate_project_ids',
    'alternate_order_numbers',
    'alternate_d365_opportunity_codes',
    'alternate_legacy_access_project_ids',
    'alternate_property_codes'
  ];
BEGIN
  FOR lr IN SELECT key, value FROM jsonb_each(coalesce(p_loser, '{}'::jsonb)) LOOP
    IF lr.key = ANY(v_meta_keys) THEN
      CONTINUE;
    END IF;
    IF NOT (v_result ? lr.key) THEN
      v_result := v_result || jsonb_build_object(lr.key, lr.value);
      v_keys_added := v_keys_added || to_jsonb(lr.key);
    ELSIF (v_result->lr.key) = lr.value THEN
      CONTINUE;
    ELSE
      v_existing_ref := v_merged_refs->lr.key;
      IF v_existing_ref IS NULL OR jsonb_typeof(v_existing_ref) <> 'array' THEN
        v_new_ref := jsonb_build_array(v_result->lr.key, lr.value);
      ELSE
        v_new_ref := v_existing_ref;
        IF NOT (v_new_ref @> jsonb_build_array(lr.value)) THEN
          v_new_ref := v_new_ref || jsonb_build_array(lr.value);
        END IF;
        IF NOT (v_new_ref @> jsonb_build_array(v_result->lr.key)) THEN
          v_new_ref := jsonb_build_array(v_result->lr.key) || v_new_ref;
        END IF;
      END IF;
      v_merged_refs := v_merged_refs || jsonb_build_object(lr.key, v_new_ref);
      v_combined := v_combined || jsonb_build_array(jsonb_build_object(
        'key', lr.key,
        'survivor', v_result->lr.key,
        'loser', lr.value,
        'merged_refs', v_new_ref
      ));
    END IF;
  END LOOP;

  IF v_merged_refs <> '{}'::jsonb THEN
    v_result := v_result || jsonb_build_object('merged_refs', v_merged_refs);
  END IF;

  RETURN jsonb_build_object(
    'merged',             v_result,
    'keys_added',         v_keys_added,
    'combined_conflicts', v_combined
  );
END;
$$;

-- When both registry rows have different scalar identifiers, keep survivor
-- primary and stash loser's value in external_ids alternate_* arrays.
CREATE OR REPLACE FUNCTION public.iqid_combine_scalar_identifiers(
  p_entity_type  public.iqid_entity_type,
  p_survivor_row jsonb,
  p_loser_row    jsonb,
  p_ext          jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  v_ext       jsonb := coalesce(p_ext, '{}'::jsonb);
  v_combined  jsonb := '[]'::jsonb;
  v_surv_text text;
  v_los_text  text;
  v_surv_int  bigint;
  v_los_int   bigint;
BEGIN
  IF p_entity_type = 'project' THEN
    v_surv_text := nullif(trim(p_survivor_row->>'project_id'), '');
    v_los_text  := nullif(trim(p_loser_row->>'project_id'), '');
    IF v_surv_text IS NOT NULL AND v_los_text IS NOT NULL AND v_surv_text <> v_los_text THEN
      v_ext := public.iqid_ext_append_unique(v_ext, 'alternate_project_ids', to_jsonb(v_los_text));
      v_combined := v_combined || jsonb_build_array(jsonb_build_object(
        'col', 'project_id', 'survivor', v_surv_text, 'loser', v_los_text,
        'stored_at', 'external_ids.alternate_project_ids'
      ));
    END IF;

    v_surv_text := nullif(trim(p_survivor_row->>'order_number'), '');
    v_los_text  := nullif(trim(p_loser_row->>'order_number'), '');
    IF v_surv_text IS NOT NULL AND v_los_text IS NOT NULL AND v_surv_text <> v_los_text THEN
      v_ext := public.iqid_ext_append_unique(v_ext, 'alternate_order_numbers', to_jsonb(v_los_text));
      v_combined := v_combined || jsonb_build_array(jsonb_build_object(
        'col', 'order_number', 'survivor', v_surv_text, 'loser', v_los_text,
        'stored_at', 'external_ids.alternate_order_numbers'
      ));
    END IF;

    v_surv_text := nullif(trim(p_survivor_row->>'d365_opportunity_code'), '');
    v_los_text  := nullif(trim(p_loser_row->>'d365_opportunity_code'), '');
    IF v_surv_text IS NOT NULL AND v_los_text IS NOT NULL AND v_surv_text <> v_los_text THEN
      v_ext := public.iqid_ext_append_unique(v_ext, 'alternate_d365_opportunity_codes', to_jsonb(v_los_text));
      v_combined := v_combined || jsonb_build_array(jsonb_build_object(
        'col', 'd365_opportunity_code', 'survivor', v_surv_text, 'loser', v_los_text,
        'stored_at', 'external_ids.alternate_d365_opportunity_codes'
      ));
    END IF;

    IF (p_survivor_row->>'legacy_access_project_id') IS NOT NULL
       AND (p_loser_row->>'legacy_access_project_id') IS NOT NULL
       AND (p_survivor_row->>'legacy_access_project_id')::bigint
           <> (p_loser_row->>'legacy_access_project_id')::bigint THEN
      v_ext := public.iqid_ext_append_unique(
        v_ext,
        'alternate_legacy_access_project_ids',
        p_loser_row->'legacy_access_project_id'
      );
      v_combined := v_combined || jsonb_build_array(jsonb_build_object(
        'col', 'legacy_access_project_id',
        'survivor', p_survivor_row->'legacy_access_project_id',
        'loser', p_loser_row->'legacy_access_project_id',
        'stored_at', 'external_ids.alternate_legacy_access_project_ids'
      ));
    END IF;
  ELSIF p_entity_type = 'property' THEN
    v_surv_text := nullif(trim(p_survivor_row->>'property_code'), '');
    v_los_text  := nullif(trim(p_loser_row->>'property_code'), '');
    IF v_surv_text IS NOT NULL AND v_los_text IS NOT NULL AND v_surv_text <> v_los_text THEN
      v_ext := public.iqid_ext_append_unique(v_ext, 'alternate_property_codes', to_jsonb(v_los_text));
      v_combined := v_combined || jsonb_build_array(jsonb_build_object(
        'col', 'property_code', 'survivor', v_surv_text, 'loser', v_los_text,
        'stored_at', 'external_ids.alternate_property_codes'
      ));
    END IF;
  END IF;

  RETURN jsonb_build_object('ext', v_ext, 'combined', v_combined);
END;
$$;

-- ── Patch dry-run: replace external_ids section + add combine_identifiers ──
-- Re-run full function from coalesce migration with these substitutions.
-- (Full bodies below mirror migration-iqid-coalesce-on-merge.sql.)

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
  v_ext_merge      jsonb;
  v_ext_result     jsonb;
  v_keys_added     jsonb;
  v_combined_ext   jsonb;
  v_scalar_combine jsonb;
  v_coalesce_fields jsonb;
  v_blacklist      text[] := ARRAY[
    'id', 'iqid', 'external_ids', 'notes', 'geo_point',
    'created_at', 'updated_at', 'created_by', 'updated_by',
    'normalized_name', 'display_name',
    'property_status', 'project_status', 'is_active'
  ];
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
    EXECUTE format('SELECT (%I)::text FROM public.%I WHERE id = $1',
                   fk_row.target_column, v_registry_table)
      INTO v_loser_fk_value USING p_loser_id;
    IF v_loser_fk_value IS NULL THEN CONTINUE; END IF;
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

  v_loser_ext    := coalesce(v_loser_row->'external_ids',    '{}'::jsonb);
  v_survivor_ext := coalesce(v_survivor_row->'external_ids', '{}'::jsonb);
  v_ext_merge    := public.iqid_merge_external_ids(v_survivor_ext, v_loser_ext);
  v_ext_result   := v_ext_merge->'merged';
  v_keys_added   := v_ext_merge->'keys_added';
  v_combined_ext := v_ext_merge->'combined_conflicts';

  v_scalar_combine := public.iqid_combine_scalar_identifiers(
    p_entity_type, v_survivor_row, v_loser_row, v_ext_result
  );
  v_ext_result := v_scalar_combine->'ext';

  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind',         'external_ids_merge',
    'loser_keys',   (SELECT coalesce(jsonb_agg(k), '[]'::jsonb) FROM jsonb_object_keys(v_loser_ext) AS k),
    'survivor_keys',(SELECT coalesce(jsonb_agg(k), '[]'::jsonb) FROM jsonb_object_keys(v_survivor_ext) AS k),
    'keys_added',   v_keys_added,
    'combined_conflicts', v_combined_ext,
    'merge_policy', 'additive keys; conflicting values combined in external_ids.merged_refs[key][] (survivor stays primary at key)'
  ));

  IF jsonb_array_length(coalesce(v_scalar_combine->'combined', '[]'::jsonb)) > 0 THEN
    v_actions := v_actions || jsonb_build_array(jsonb_build_object(
      'kind',     'combine_identifiers',
      'combined', v_scalar_combine->'combined',
      'merge_policy', 'survivor column wins; loser value stored in external_ids alternate_* array'
    ));
  END IF;

  v_coalesce_fields := (
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'col',   l.key,
      'value', l.value
    ) ORDER BY l.key), '[]'::jsonb)
    FROM jsonb_each(v_loser_row) l
    WHERE l.value IS NOT NULL
      AND jsonb_typeof(l.value) <> 'null'
      AND NOT (l.key = ANY(v_blacklist))
      AND (
        (v_survivor_row -> l.key) IS NULL
        OR jsonb_typeof(v_survivor_row -> l.key) = 'null'
      )
  );
  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind',          'coalesce_fields',
    'fields_filled', v_coalesce_fields,
    'merge_policy',  'survivor wins when NOT NULL; loser fills NULL survivor columns'
  ));

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

  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind',       'alias_insert',
    'alias_name', v_loser_row->>v_name_column,
    'iqid',       v_survivor_row->>'iqid'
  ));

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
  v_ext_merge        jsonb;
  v_scalar_combine   jsonb;
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
  v_coalesce_set     text[] := ARRAY[]::text[];
  v_coalesce_fields  jsonb;
  v_blacklist        text[] := ARRAY[
    'id', 'iqid', 'external_ids', 'notes', 'geo_point',
    'created_at', 'updated_at', 'created_by', 'updated_by',
    'normalized_name', 'display_name',
    'property_status', 'project_status', 'is_active'
  ];
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

  EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1', v_registry_table)
    INTO v_loser_row USING p_loser_id;
  EXECUTE format('SELECT to_jsonb(t) FROM public.%I t WHERE id = $1', v_registry_table)
    INTO v_survivor_row USING p_survivor_id;

  IF v_loser_row    IS NULL THEN RAISE EXCEPTION 'Loser % not found in %',    p_loser_id,    v_registry_table; END IF;
  IF v_survivor_row IS NULL THEN RAISE EXCEPTION 'Survivor % not found in %', p_survivor_id, v_registry_table; END IF;

  v_loser_name := v_loser_row->>v_name_column;

  v_survivor_iqid := v_survivor_row->>'iqid';
  IF v_survivor_iqid IS NULL THEN
    SELECT public.iqid_mint(p_entity_type) INTO v_survivor_iqid;
    EXECUTE format('UPDATE public.%I SET iqid = $1 WHERE id = $2', v_registry_table)
      USING v_survivor_iqid, p_survivor_id;
    v_minted_iqid := true;
  END IF;

  v_loser_ext    := coalesce(v_loser_row->'external_ids',    '{}'::jsonb);
  v_survivor_ext := coalesce(v_survivor_row->'external_ids', '{}'::jsonb);
  v_ext_merge    := public.iqid_merge_external_ids(v_survivor_ext, v_loser_ext);
  v_merged_ext   := v_ext_merge->'merged';

  v_scalar_combine := public.iqid_combine_scalar_identifiers(
    p_entity_type, v_survivor_row, v_loser_row, v_merged_ext
  );
  v_merged_ext := v_scalar_combine->'ext';

  IF v_merged_ext <> v_survivor_ext THEN
    EXECUTE format('UPDATE public.%I SET external_ids = $1 WHERE id = $2', v_registry_table)
      USING v_merged_ext, p_survivor_id;
  END IF;

  v_actions := v_actions || jsonb_build_array(jsonb_build_object(
    'kind',               'external_ids_merge',
    'keys_added',         v_ext_merge->'keys_added',
    'combined_conflicts', v_ext_merge->'combined_conflicts',
    'merge_policy',       'additive keys; conflicts combined in merged_refs[key][]'
  ));

  IF jsonb_array_length(coalesce(v_scalar_combine->'combined', '[]'::jsonb)) > 0 THEN
    v_actions := v_actions || jsonb_build_array(jsonb_build_object(
      'kind',     'combine_identifiers',
      'combined', v_scalar_combine->'combined',
      'merge_policy', 'survivor column primary; loser in alternate_* arrays'
    ));
  END IF;

  v_coalesce_fields := (
    SELECT coalesce(jsonb_agg(jsonb_build_object(
      'col',   l.key,
      'value', l.value
    ) ORDER BY l.key), '[]'::jsonb)
    FROM jsonb_each(v_loser_row) l
    WHERE l.value IS NOT NULL
      AND jsonb_typeof(l.value) <> 'null'
      AND NOT (l.key = ANY(v_blacklist))
      AND (
        (v_survivor_row -> l.key) IS NULL
        OR jsonb_typeof(v_survivor_row -> l.key) = 'null'
      )
  );

  IF jsonb_array_length(v_coalesce_fields) > 0 THEN
    SELECT array_agg(format('%I = COALESCE(s.%I, l.%I)',
                            elem->>'col', elem->>'col', elem->>'col'))
      INTO v_coalesce_set
    FROM jsonb_array_elements(v_coalesce_fields) AS elem;

    EXECUTE format(
      'UPDATE public.%I s SET %s FROM public.%I l WHERE s.id = $1 AND l.id = $2',
      v_registry_table,
      array_to_string(v_coalesce_set, ', '),
      v_registry_table
    ) USING p_survivor_id, p_loser_id;

    v_actions := v_actions || jsonb_build_array(jsonb_build_object(
      'kind',          'coalesce_fields',
      'fields_filled', v_coalesce_fields,
      'merge_policy',  'survivor wins when NOT NULL; loser filled NULL survivor columns'
    ));
  ELSE
    v_actions := v_actions || jsonb_build_array(jsonb_build_object(
      'kind',          'coalesce_fields',
      'fields_filled', '[]'::jsonb,
      'merge_policy',  'no NULL survivor columns had loser values to fill'
    ));
  END IF;

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
      v_actions := v_actions || jsonb_build_array(jsonb_build_object(
        'kind', 'alias_insert_skipped', 'alias_name', v_loser_name,
        'reason', 'already-known alias'
      ));
    END;
  END IF;

  v_status_to_set := CASE p_entity_type
    WHEN 'property' THEN 'inactive'
    WHEN 'project'  THEN 'inactive'
    ELSE NULL
  END;

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

COMMENT ON FUNCTION public.iqid_merge_external_ids(jsonb, jsonb) IS
  'Merge two external_ids objects: additive keys; conflicting values preserved in merged_refs[key][] with survivor primary at key.';

COMMENT ON FUNCTION public.iqid_combine_scalar_identifiers(public.iqid_entity_type, jsonb, jsonb, jsonb) IS
  'When both registry rows have different scalar identifiers (project_id, order_number, etc.), keep survivor column and append loser to external_ids alternate_* arrays.';

COMMENT ON FUNCTION public.iqid_apply_merge(public.iqid_entity_type, uuid, uuid, text) IS
  'Executes merge: mint iqid, combine external_ids + scalar identifiers, coalesce NULLs, FK repoints, alias, soft-delete loser.';

COMMENT ON FUNCTION public.iqid_dry_run_merge(public.iqid_entity_type, uuid, uuid) IS
  'Dry-run merge plan including combined identifiers preview (merged_refs + alternate_* arrays).';
