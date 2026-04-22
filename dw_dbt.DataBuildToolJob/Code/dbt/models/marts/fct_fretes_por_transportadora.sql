{{ config(materialized='table') }}

select
    transportadora,
    count(*)                as qtd_fretes,
    sum(valor_frete)        as valor_total,
    avg(valor_frete)        as ticket_medio,
    sum(peso_kg)            as peso_total_kg
from {{ ref('stg_fretes') }}
group by transportadora
