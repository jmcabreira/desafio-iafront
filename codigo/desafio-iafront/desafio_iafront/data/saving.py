import os
from typing import Sequence
from uuid import uuid4

from desafio_iafront.jobs.pedidos.utils import _prepare
from desafio_iafront.jobs.pedidos.contants import SAVING_PARTITIONS

import pandas as pd

_default = "__HIVE_DEFAULT_PARTITION__"


def save_partitioned(dataframe: pd.DataFrame, save_path: str, partitions: Sequence[str] = []) -> None:
    column = partitions[0]
    # Pega os valores possíveis da coluna atual
    for value in list(dataframe[column].unique()):
        # para cada valor da coluna, filtra o dataset
        filtered = dataframe[dataframe[column] == value].drop(columns=column)
        column_value = _default if value is None else value
        path_with_partition = os.path.join(save_path, f"{column}={column_value}")
        os.makedirs(path_with_partition, exist_ok=True)

        # Se existe outra coluna além do atual, prossegue o particionamento
        if partitions[1:]:
            # remove a coluna atual da lista
            save_partitioned(filtered, path_with_partition, partitions[1:])
        else:
            # se não há mais colunas, salva
            filtered.to_json(os.path.join(path_with_partition, f"{str(uuid4())}.json"),
                             orient='records', lines=True, force_ascii=False, date_unit="s")

# ===================================== Cabreira's Code ====================================================
def save_prepared(saida: str, visita_com_produto_e_conversao_df: pd.DataFrame):
    
    prepared = _prepare(visita_com_produto_e_conversao_df)
    save_partitioned(prepared, saida, SAVING_PARTITIONS)
