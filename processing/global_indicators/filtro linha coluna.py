import pandas as pd
import csv

filtrado = pd.read_csv("filtrado.csv")

filtrado_transformado = filtrado.melt(
    id_vars=['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code'],
    var_name='Year',
    value_name='Value'
)

filtrado_transformado.to_csv("filtrado_transformado.csv", index=False, quoting=csv.QUOTE_ALL)
