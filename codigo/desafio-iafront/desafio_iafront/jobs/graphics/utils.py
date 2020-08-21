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

def hist_plot(dataframe: pd.DataFrame, axis, title=""):
    
    p = figure(title=title, tools='',background_fill_color="#fafafa")
    
    hist,edges = np.histogram(dataframe[axis],  density= True, bins = 50)
    
    p.quad(top=hist, bottom=0, left=edges[:-1], right=edges[1:],fill_color='navy', line_color = 'white', alpha=0.5 , legend_label = axis)
    
    p.xaxis.axis_label = axis
    p.yaxis.axis_label = 'f(x)'
    p.grid.grid_line_color="white"
    
    return p