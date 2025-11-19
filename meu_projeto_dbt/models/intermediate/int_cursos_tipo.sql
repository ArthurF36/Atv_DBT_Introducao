{{ config(materialized='view') }}

with union_all_years as (

    select
        2017 as nu_ano_censo,
        tp_rede,
        sg_uf
    from {{ ref('stg_cadastro_cursos_2017') }}

    union all
    select
        2018 as nu_ano_censo,
        tp_rede,
        sg_uf
    from {{ ref('stg_cadastro_cursos_2018') }}

    union all
    select
        2019 as nu_ano_censo,
        tp_rede,
        sg_uf
    from {{ ref('stg_cadastro_cursos_2019') }}

    union all
    select
        2022 as nu_ano_censo,
        tp_rede,
        sg_uf
    from {{ ref('stg_cadastro_cursos_2022') }}

    union all
    select
        2023 as nu_ano_censo,
        tp_rede,
        sg_uf
    from {{ ref('stg_cadastro_cursos_2023') }}

    union all
    select
        2024 as nu_ano_censo,
        tp_rede,
        sg_uf
    from {{ ref('stg_cadastro_cursos_2024') }}
),

padronizado as (
    select
        nu_ano_censo,
        upper(sg_uf) as sg_uf,
        tp_rede,
        case tp_rede
            when 1 then 'Pública'
            when 2 then 'Privada'
            else 'Não informado'
        end as rede_desc
    from union_all_years
)

select *
from padronizado;