{{ config(materialized='view') }}

select
    cast("TP_REDE" as integer) as tp_rede,
    cast("SG_UF"   as text)    as sg_uf
from {{ source('raw', 'MICRODADOS_CADASTRO_CURSOS_2019') }};