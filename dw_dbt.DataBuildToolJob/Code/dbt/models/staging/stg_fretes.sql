{{ config(materialized='view') }}

select
    frete_id,
    cast(data_emissao as date) as data_emissao,
    transportadora,
    origem_uf,
    destino_uf,
    cast(valor_frete as decimal(12,2)) as valor_frete,
    cast(peso_kg as decimal(10,2)) as peso_kg
from (
    values
        (1, '2026-04-01', 'TransLog',   'SC', 'SP',  1250.50, 320.00),
        (2, '2026-04-02', 'TransLog',   'SC', 'RJ',  1890.00, 510.50),
        (3, '2026-04-02', 'RodoFrete',  'PR', 'SP',   980.75, 215.00),
        (4, '2026-04-03', 'RodoFrete',  'PR', 'MG',  1450.00, 400.00),
        (5, '2026-04-03', 'ExpressCar', 'RS', 'SP',  2100.00, 610.25),
        (6, '2026-04-04', 'TransLog',   'SC', 'BA',  3200.00, 780.00),
        (7, '2026-04-05', 'ExpressCar', 'RS', 'PR',   720.00, 150.00)
) as v (frete_id, data_emissao, transportadora, origem_uf, destino_uf, valor_frete, peso_kg)
