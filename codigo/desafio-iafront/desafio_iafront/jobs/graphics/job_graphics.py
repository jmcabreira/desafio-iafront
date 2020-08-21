import click
from bokeh.io import output_file, save
#from functools import partial
from bokeh.io import output_file, save, show
import os

from desafio_iafront.jobs.graphics.utils import plot,scatter_plot,hist_plot
from desafio_iafront.data.dataframe_utils import read_partitioned_json,read_data_partitions
#from desafio_iafront.jobs.common import filter_date



@click.command()
@click.option('--data-path', type=click.Path(exists=True))
#@click.option('--saida', type=click.Path(exists=False, dir_okay=True, file_okay=False))
@click.option('--x_axis')
@click.option('--y_axis')
@click.option('--data-inicial', type=click.DateTime(formats=["%d/%m/%Y"]))
@click.option('--data-final', type=click.DateTime(formats=["%d/%m/%Y"]))
@click.option('--file_output_name')
@click.option('--plot-type',type=click.Choice(['scatter','hist'], case_sensitive=False),default = 'hist')
def main(data_path: str, x_axis:str, y_axis:str , data_inicial , data_final, file_output_name : str, plot_type:str):
    
    plot_type = plot_type.strip().lower()
    dataframe = read_data_partitions(data_inicial,data_final, data_path)
    
    data_dir = './graphs/standard-plots/' 
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    print('Plotting Graph')
    if plot_type == 'scatter':
        figura = scatter_plot(dataframe, x_axis, y_axis)
        
    elif plot_type =='hist':
        figura = hist_plot(dataframe, x_axis)

    output_file(str(data_dir)+ str(file_output_name)+".html" )
    print('Saving graph') 
    show(figura)

if __name__ == '__main__':
    main()