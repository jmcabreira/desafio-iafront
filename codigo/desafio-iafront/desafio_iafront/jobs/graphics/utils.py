import pandas as pd
from bokeh.plotting import figure
import numpy as np



def plot(dataframe: pd.DataFrame, x_axis, y_axis, cluster_label, title=""):
    clusters = [label for label in dataframe[cluster_label]]

    colors = [set_color(_) for _ in clusters]

    p = figure(title=title)

    p.scatter(dataframe[x_axis].tolist(), dataframe[y_axis].tolist(), fill_color=colors)

    return p


def _unique(original):
    return list(set(original))


def set_color(color):
    COLORS = ["green", "blue", "red", "orange", "purple"]

    index = color % len(COLORS)

    return COLORS[index]
#================================== Cabreira's Code =============================

def scatter_plot(dataframe: pd.DataFrame, x_axis, y_axis, title=""):
    
    p = figure(title=title, tools='',background_fill_color="#fafafa")

    p.scatter(dataframe[x_axis].tolist(), dataframe[y_axis].tolist())
    p.xaxis.axis_label = x_axis
    p.yaxis.axis_label = y_axis
    p.grid.grid_line_color= "white"

    return p

def hist_plot(dataframe: pd.DataFrame, x_axis, title=""):
    
    p = figure(title=title, tools='',background_fill_color="#fafafa")
    
    hist,edges = np.histogram(dataframe[x_axis],  density= True, bins = 50)
    
    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:],fill_color='navy', line_color = 'white', alpha=0.5 , legend_label = axis)
    
    p.xaxis.axis_label = axis
    p.yaxis.axis_label = 'f(x)'
    p.grid.grid_line_color="white"
    
    return p

def scaled_scatter_plot(dataframe: pd.DataFrame, x_axis, y_axis, title=""):
    
    x_axis_data = get_scaled_data(dataframe, x_axis)
    y_axis_data = get_scaled_data(dataframe, y_axis)

    
    p = figure(title=title, tools='',background_fill_color="#fafafa")

    p.scatter(list(x_axis_data), list(y_axis_data))
    p.xaxis.axis_label = x_axis
    p.yaxis.axis_label = y_axis
    p.grid.grid_line_color= "white"
    

    return p

def scaled_hist_plot(dataframe: pd.DataFrame, axis, title=""):
    
    x_axis_data = get_scaled_data(dataframe, axis)
    
    p = figure(title=title, tools='',background_fill_color="#fafafa")
    
    hist,edges = np.histogram(list(x_axis_data),  density= True, bins = 50)
    
    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:],fill_color='navy', line_color = 'white', alpha=0.5 , legend_label = axis)
    
    p.xaxis.axis_label = axis
    p.yaxis.axis_label = 'f(x)'
    p.grid.grid_line_color="white"
    
    show(p)
    
    return p

def specify_axis_position(axis):
    variables = ['preco', 'prazo', 'frete', 'latitude', 'longitude']
    variable_index =  variables.index(axis)
    return variable_index

def get_scaled_data(dataframe, axis):
    index = specify_axis_position(axis)
    scaled_data = list(zip(*dataframe.features))[index]
    return scaled_data