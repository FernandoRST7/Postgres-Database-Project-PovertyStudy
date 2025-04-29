import pandas as pd

# Carregar o CSV
df = pd.read_csv("processing/global_indicators/Global_Indicators_filtered.csv")

# Criar uma cópia explícita do slice
df_region = df.iloc[:1617].copy()  # <--- Adicione .copy() aqui

# Renomear as colunas (agora seguro)
df_region.rename(columns={'country_name': 'region_name', 'country_code': 'region_code'}, inplace=True)

# Selecionar apenas as colunas desejadas
colunas = ['region_name', 'region_code']
df_region = df_region[colunas]

# Salvar o resultado
df_region.to_csv("processing/global_indicators/Global_Indicators_regions_filtered.csv", index=False)

print("Arquivo Global_Indicators_regions_filtered.csv gerado com sucesso!")
