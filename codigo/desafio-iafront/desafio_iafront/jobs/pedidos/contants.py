from desafio_iafront.jobs.contants import COLUNA_DATA, \
    COLUNA_HORA, COLUNA_DEPARTAMENTO

SAVING_PARTITIONS = [COLUNA_DEPARTAMENTO, COLUNA_DATA, COLUNA_HORA]
ENCODE_COLUMNS = ["departamento"]

KEPT_COLUNS = ['product_id', 'visit_id', "purchase_id", 'datetime', 'product_category_name', 'prazo',
               "preco", "frete", "cep_prefixo", "coordenadas", 'data', 'hora']

COLUMN_RENAMES = {
    'product_id': "id_produto",
    'visit_id': "id_visita",
    'datetime': 'datahora',
    'product_category_name': 'departamento',
    'delivery_date': 'data_entrega',
    'purchase_id': 'id_pedido'
}
