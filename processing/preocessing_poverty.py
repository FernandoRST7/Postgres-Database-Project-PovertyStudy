import pandas as pd
import numpy as np
caminho_salvar = "processing\Poverty_Inequality_changed.csv"

# Ler o CSV
df = pd.read_csv('processing\Poverty_Inequality.csv')

# Excluir colunas
columns_to_drop = [
    'region_name', 'survey_year', 'watts', 'mld', 'polarization', 
    'cpi', 'ppp', 'is_interpolated', 'estimation_type', 'estimate_type'
]
df = df.drop(columns_to_drop, axis=1)

# Lista de colunas que devem permanecer como estão (não convertidas para float)
protected_columns = [
    'region_code', 'country_name', 'country_code', 'reporting_level',
    'survey_acronym', 'survey_coverage', 'welfare_type', 'comparable_spell', 'distribution_type'
]

# Converter colunas numéricas para float
for coluna in df.columns:
    if coluna not in protected_columns:
        df[coluna] = df[coluna].astype(float)
# - Converter tipos (ex: string para float)


# 3. Salvar o DataFrame processado em um novo CSV
df.to_csv(caminho_salvar, index=False)  # index=False evita salvar uma coluna extra de índices

"""
# Exibir informações básicas
print("Primeiras linhas do DataFrame:")
print(df.head())

print("\nInformações do DataFrame:")
print(df.info())

# Converter para NumPy array
array_numpy = df.to_numpy()

print("\nArray NumPy:")
print(array_numpy) 
"""


