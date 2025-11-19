{{ config(materialized='table') }}

with base as (
    select
        nu_ano_censo,
        rede_desc,
        tp_rede
    from {{ ref('int_cadastro_cursos') }}
),

aggregated as (
    select
        nu_ano_censo,
        rede_desc,
        tp_rede,
        count(*) as total_cursos
    from base
    group by 1, 2, 3
)

select *
from aggregated
order by nu_ano_censo, tp_rede;