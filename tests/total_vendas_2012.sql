with
    vendas_em_2012 as (
        select sum(valor_bruto) as total_bruto
        from {{ ref('fct_vendas') }}
        where DATA_DO_PEDIDO between '2012-01-01' and '2012-12-31'
    )

select total_bruto
from vendas_em_2012
where total_bruto not between 226298 and 226299