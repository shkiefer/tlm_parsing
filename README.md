# Spektrum TLM File Parsing Functions
A set of functions for parsing Spektrum TLM files.  
This is a simple set of functions written in python to extract data from Spektrum telementry files (TLM) written to the SD card or internal memory on the transmitter.
Copy the file from your transmittter to the computer and use these functions to parse the data and commence data wrangling!

The parsing functions returning dictionaries was on purpose, potentially for use with a database / API with data 'models' setup for each sensor.  

A webapp developed to showcase the usage of this parsing is available at [PlotMyRC.com](https://www.plotmyrc.com)


## Example usage (VScode Jupyter/Interactive Window)

```python

# %% Import dependencies and parse the file
import numpy as np
import pandas as pd
from pathlib import Path


# copy all parsing functions here, or import from relative file


main_header_dics, supplemental_header_dics, data_dics = parse_tlm_file(Path('path-to-your-filename.TLM'))

# meta data for the file
df_main_headers = pd.DataFrame(main_header_dics)
# supplemental header data includes which sensors are included in the data
df_supplemental_headers = pd.DataFrame(supplemental_header_dics)
# sparse assembled data frame
df_data = assemble_tlm_data(data_dics)


# %% basic interactive plot of some common data
import plotly.graph_objects as go

session_id = 1
cols = ['esc_vInput', 'esc_throttle_%']

df_plot = df_data.sort_values('timestamp_ms').loc[df_data['Session ID'] == session_id].dropna(subset=cols, how='all').set_index('timestamp_ms', drop=True).reindex(columns=cols)

fig = go.Figure()
fig.update_xaxes(title='Elapsed Time (s)')

# relative elapsed time from timestamp
x = (df_plot.index - df_plot.index[0]).values / 1000.

fig.add_trace(
    go.Scatter(
        x = x,
        y = df_plot[cols[0]],
        name=cols[0]
    )
)
fig.add_trace(
    go.Scatter(
        x = x,
        y = df_plot[cols[1]],
        name=cols[1],
        yaxis='y2'
    )
)
fig.update_layout(
    yaxis=dict(
        title=dict(
            text='ESC Battery Voltage (V)'
        )
    ),
    yaxis2=dict(
        title=dict(
            text='Throttle Input (%)'
        ),
        anchor='free',
        overlaying="y",
        autoshift=True

    )
)
fig.show()

```