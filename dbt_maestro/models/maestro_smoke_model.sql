-- Minimal model: no sources, no seeds, so the run cannot fail for data reasons.
-- Proves dbt executed against the warehouse and materialized a table.
select
    1 as id,
    'maestro' as source_name,
    current_timestamp() as loaded_at
