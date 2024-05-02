with
    pedido_por_itens as (
        select *
        from {{ ref('int_pedidos_por_itens') }}
    )

    , dim_produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , dim_funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , joined as (
        select
            fatos.PK_VENDAS
            , fatos.FK_PEDIDO as numero_nota_fiscal
            , fatos.FK_PRODUTO
            , fatos.FK_CLIENTE
            , fatos.FK_FUNCIONARIO
            , fatos.FK_TRANSPORTADORA
            , fatos.PRECO_DA_UNIDADE
            , fatos.QUANTIDADE
            , fatos.DESCONTO_PERC
            , fatos.DATA_DO_PEDIDO
            , fatos.DATA_REQUERIDA_ENTREGA
            , fatos.DATA_DO_ENVIO
            , fatos.FRETE
            , fatos.NM_DESTINATARIO
            , fatos.CIDADE_DESTINATARIO
            , fatos.REGIAO_DESTINATARIO
            , fatos.PAIS_DESTINATARIO
            , dim_produtos.NM_PRODUTO
            , dim_produtos.QUANTIDADE_POR_UNIDADE
            , dim_produtos.IS_DISCONTINUADO
            , dim_produtos.NM_CATEGORIA
            , dim_produtos.DESCRICAO_CATEGORIA
            , dim_produtos.NM_FORNECEDOR
            , dim_produtos.CIDADE_FORNECEDOR
            , dim_produtos.PAIS_FORNECEDOR
            , dim_funcionarios.NM_FUNCIONARIO
            , dim_funcionarios.CARGO_FUNCIONARIO
            , dim_funcionarios.DT_CONTRATACAO
            , dim_funcionarios.NM_GERENTE
        from pedido_por_itens as fatos
        left join dim_produtos on fatos.FK_PRODUTO = dim_produtos.pk_produto
        left join dim_funcionarios on fatos.FK_FUNCIONARIO = dim_funcionarios.pk_funcionario

    )

    , metricas as (
        select
            *
            , QUANTIDADE * PRECO_DA_UNIDADE as valor_bruto
            , QUANTIDADE * (1-DESCONTO_PERC)* PRECO_DA_UNIDADE as valor_liquido
            , cast(
                (DT_CONTRATACAO - DATA_DO_PEDIDO) / 365
                as numeric(18,0)
            ) as senioridade_em_anos
            , cast(
                FRETE / count(numero_nota_fiscal) over (partition by numero_nota_fiscal)
                as numeric(18,2)
            ) as frete_rateado
        from joined
    )

select *
from metricas