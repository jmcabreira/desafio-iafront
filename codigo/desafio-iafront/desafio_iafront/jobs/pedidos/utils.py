import pandas as pd

from desafio_iafront.jobs.pedidos.contants import KEPT_COLUNS, COLUMN_RENAMES


def _prepare(pedidos_joined: pd.DataFrame) -> pd.DataFrame:
    # Remove colunas resultantes do merge
    result_dataset = drop_merged_columns(pedidos_joined)
    # Remove colunas que não serão usadas
    result_dataset = result_dataset[KEPT_COLUNS]
    # Renomeia colunas
    result_dataset = result_dataset.rename(columns=COLUMN_RENAMES)

    return result_dataset


def drop_merged_columns(data_frame: pd.DataFrame) -> pd.DataFrame:
    result_dataset = data_frame.copy(deep=True)
    for column in data_frame.columns:
        if column.endswith("_off"):
            result_dataset = data_frame.drop(column, axis=1)
    return result_dataset
