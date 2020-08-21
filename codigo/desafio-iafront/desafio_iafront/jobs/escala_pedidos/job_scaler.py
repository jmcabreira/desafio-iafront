import click
from sklearn.preprocessing import Normalizer,StandardScaler,MinMaxScaler,MaxAbsScaler,RobustScaler,PowerTransformer
import os
from desafio_iafront.data.saving import save_partitioned
from desafio_iafront.jobs.common import prepare_dataframe, transform
from desafio_iafront.jobs.escala_pedidos.constants import DEPARTAMENTOS

from sklearn.base import TransformerMixin

@click.command()
@click.option('--visitas-com-conversao', type=click.Path(exists=True))
@click.option('--saida', type=click.Path(exists=False, dir_okay=True, file_okay=False))
@click.option('--data-inicial', type=click.DateTime(formats=["%d/%m/%Y"]))
@click.option('--data-final', type=click.DateTime(formats=["%d/%m/%Y"]))
@click.option('--departamentos', type=str, default = DEPARTAMENTOS, help="Departamentos separados por virgula")
@click.option('--scaler',type=click.Choice(['Normalizer', 'StandardScaler',
 'MinMaxScaler', 'MaxAbsScaler', 'RobustScaler','PowerTransformer'], case_sensitive=False),default = 'Normalizer')
def main(visitas_com_conversao, saida, data_inicial, data_final, departamentos, scaler):
    
    if type(departamentos) == str:
        departamentos = [departamento.strip() for departamento in departamentos.split(",")]

    result = prepare_dataframe(departamentos, visitas_com_conversao, data_inicial, data_final) 
    scaler = scaler.lower().strip()

    # Faz a escala dos valores
    if scaler == 'standardscaler':
        result_scaled = transform(result, StandardScaler()) 
    if scaler == 'minmaxscaler':
        result_scaled = transform(result, StandardScaler())
    if scaler == 'maxabsscaler': 
        result_scaled = transform(result, MinMaxScaler())
    if scaler == 'robustscaler':
        result_scaled = transform(result, RobustScaler())
    if scaler == 'powertransformer':
        result_scaled = transform(result, PowerTransformer())
    else:
        result_scaled = transform(result, Normalizer())
        
    #return result_scaled
    #salva resultado

    data_dir = saida
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    save_partitioned(result_scaled, data_dir, ['data', 'hora'])

if __name__ == '__main__':
    main()
