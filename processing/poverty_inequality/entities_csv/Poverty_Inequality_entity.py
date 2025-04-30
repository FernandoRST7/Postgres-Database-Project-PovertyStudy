import pandas as pd

# Carregar o CSV
df = pd.read_csv('processing/poverty_inequality/Poverty_Inequality_filtered.csv')

## REGION
# Manter apenas as colunas 'region_code' e 'country_name'
df = df[['region_code', 'region_name']]
df.drop_duplicates(subset=['region_code'], inplace=True)  # Remover duplicatas por 'country_code'

# Salvar o novo DataFrame (opcional)
df.to_csv('processing/poverty_inequality/entities_csv/Region.csv', index=False)

## COUNTRY
df_poverty = pd.read_csv('processing/poverty_inequality/Poverty_Inequality_filtered.csv')
df_country = df_poverty[['region_code', 'country_name', 'country_code']]  # Para a tabela 'Country'

df_country.drop_duplicates(subset=['country_code'], inplace=True)  # Remover duplicatas por 'country_code'

with open('processing/poverty_inequality/entities_csv/Country.csv', 'w') as f1:
    df_country.to_csv(f1, index=False)