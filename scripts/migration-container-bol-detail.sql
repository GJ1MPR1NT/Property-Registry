-- Chain-iQ Supabase (project: bpibnvwviqilpuuvcgdm) — run in SQL Editor or psql.
-- Canonical copy also lives at: Chain-iQ/scripts/migration-container-bol-detail.sql
--
-- Purpose: one row per container_loads row with BOL fields denormalized from Flexport
-- so UIs can join by container_loads.id without gaps from (shipment_id, container_number) keys.

CREATE TABLE IF NOT EXISTS container_bol_detail (
  container_loads_id uuid PRIMARY KEY REFERENCES container_loads (id) ON DELETE CASCADE,
  flexport_shipment_id integer,
  container_number text,
  vendor text,
  hbl_number text,
  mbl_number text,
  carrier_booking text,
  incoterm text,
  freight_type text,
  it_number text,
  shipper_name text,
  shipper_address jsonb,
  shipper_country text,
  consignee_name text,
  consignee_address jsonb,
  consignee_country text,
  notify_party_name text,
  notify_party_address jsonb,
  total_pieces integer,
  total_weight_kg double precision,
  total_volume_cbm double precision,
  cargo_description text,
  marks_and_numbers text,
  documents jsonb,
  synced_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_container_bol_detail_shipment
  ON container_bol_detail (flexport_shipment_id);

CREATE INDEX IF NOT EXISTS idx_container_bol_detail_container_number
  ON container_bol_detail (container_number);

COMMENT ON TABLE container_bol_detail IS 'BOL snapshot per tracked container_loads row; populated by flexport_sync pass 5 (bols).';
