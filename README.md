# Projeto de Banco de Dados: Pobreza, Desigualdade e Indicadores Sociais Globais
  
## Descrição do Projeto

Este projeto integra dados de dois conjuntos de dados (datasets) relacionados a:
- [Pobreza e desigualdade global](https://pip.worldbank.org/poverty-calculator)
- [Indicadores de educação, trabalho, demografia, saúde e economia dos países](https://datacatalog.worldbank.org/search/dataset/0037712/World-Development-Indicators)

O banco de dados foi desenvolvido para armazenar, organizar e permitir consultas significativas sobre a situação social e econômica dos países, com foco em temas diretamente conectados aos **Objetivos de Desenvolvimento Sustentável (ODS)** da ONU.

---

## Objetivos de Desenvolvimento Sustentável Relacionados
Os dados e consultas abordam temas conectados aos seguintes ODS:
- ODS 1: Erradicação da Pobreza
- ODS 3: Saúde e Bem-Estar
- ODS 4: Educação de Qualidade
- ODS 10: Redução das Desigualdades  

---

### 1. Modelo Conceitual

- Desenvolvemos o diagrama entidade-relacionamento (DER) representando as principais entidades (como País, Indicador, Pobreza, Educação, Saúde, etc.) e seus relacionamentos.
- Para isso filtramos os datasets escolhidos pelo código identificador dos indicadores, separando cada um deles em entidades. Além disso atribuimos nomes melhores para cada indicador, a fim de facilitar a consulta SQL.
- Abaixo temos os indicadores escolhidos de cada dataset separado em suas entidades. Vale lembrar que a tabela abaixo tem apenas a parte dos indicadores, sendo que dada informação dessa possui seu país/região e ano de origem da pesquisa.

#### [Global Indicators](https://github.com/FernandoRST7/Postgres-Database-Project-PovertyStudy/blob/main/processing/global_indicators/Global_Indicators_filtered.csv)

| INDICATOR                                                                   | INDICATOR CODE    | STRONG ENTITY / WEAK ENTITY |      ATTRIBUTE NAME      |
| --------------------------------------------------------------------------- | ----------------- | --------------------------- | :----------------------: |
| **DEMOGRAPHY**                                                              |                   |                             |                          |
| Population ages 0-14, female                                                | SP.POP.0014.FE.IN | Demography/Population       | pop_ages, gender, number |
| Population ages 0-14, male                                                  | SP.POP.0014.MA.IN | Demography/Population       |            -             |
| Population ages 15-64, female                                               | SP.POP.1564.FE.IN | Demography/Population       |            -             |
| Population ages 15-64, male                                                 | SP.POP.1564.MA.IN | Demography/Population       |            -             |
| Population ages 65 and above, female                                        | SP.POP.65UP.FE.IN | Demography/Population       |            -             |
| Population ages 65 and above, male                                          | SP.POP.65UP.MA    | Demography/Population       |            -             |
| Population density (people per sq. km of land area)                         | EN.POP.DNST       | Demography                  |       pop_density        |
| Urban population                                                            | SP.URB.TOTL       | Demography                  |        urban_pop         |
| Rural population                                                            | SP.RUR.TOTL       | Demography                  |        rural_pop         |
| Net migration                                                               | SM.POP.NETM       | Demography                  |      net_migration       |
| Death rate, crude (per 1,000 people)                                        | SP.DYN.CDRT.IN    | Demography                  |        death_rate        |
| Birth rate, crude (per 1,000 people)                                        | SP.DYN.CBRT.IN    | Demography                  |        birth_rate        |
| Life expectancy at birth, female (years)                                    | SP.DYN.LE00.FE.IN | Demography/Life Expectancy  |      gender, value       |
| Life expectancy at birth, male (years)                                      | SP.DYN.LE00.MA.IN | Demography/Life Expectancy  |            -             |
|                                                                             |                   |                             |                          |
| **EMPLOYMENT**                                                              |                   |                             |                          |
| Children in employment, total (% of children ages 7-14)                     | SL.TLF.0714.ZS    | Employment                  |        child_emp         |
| Unemployment, total (% of total labor force) (national estimate)            | SL.UEM.TOTL.NE.ZS | Employment                  |          unemp           |
| Vulnerable employment, total (% of total employment) (modeled ILO estimate) | SL.EMP.VULN.ZS    | Employment                  |      vulnerable_emp      |
| Part time employment, total (% of total employment)                         | SL.TLF.PART.ZS    | Employment                  |        part_time         |
| Employers, total (% of total employment) (modeled ILO estimate)             | SL.EMP.MPYR.ZS    | Employment                  |        employers         |
| Labor force, total                                                          | SL.TLF.TOTL.IN    | Employment                  |    labor_force_total     |
| Labor force, female (% of total labor force)                                | SL.TLF.TOTL.FE.ZS | Employment                  |     labor_force_fem      |
|                                                                             |                   |                             |                          |
| **EDUCATION**                                                               |                   |                             |                          |
| Children out of school, primary                                             | SE.PRM.UNER       | Education                   |   child_out_of_school    |
| Progression to secondary school (%)                                         | SE.SEC.PROG.ZS    | Education                   |    progression_to_sec    |
| Government expenditure on education, total (% of GDP)                       | SE.XPD.TOTL.GD.ZS | Education                   |       expenditure        |
| School enrollment, preprimary (% gross)                                     | SE.PRE.ENRR       | Education                   |      preprim_enrol       |
| School enrollment, primary (% gross)                                        | SE.PRM.ENRR       | Education                   |        prim_enrol        |
| School enrollment, secondary (% gross)                                      | SE.SEC.ENRR       | Education                   |        sec_enrol         |
| School enrollment, tertiary (% gross)                                       | SE.TER.ENRR       | Education                   |       terti_enrol        |
|                                                                             |                   |                             |                          |
| **HEALTH**                                                                  |                   |                             |                          |
| Hospital beds (per 1,000 people)                                            | SH.MED.BEDS.ZS    | Health                      |      hospital_beds       |
| Physicians (per 1,000 people)                                               | SH.MED.PHYS.ZS    | Health                      |        physicians        |
| Current health expenditure (% of GDP)                                       | SH.XPD.CHEX.GD.ZS | Health                      |       expenditure        |
|                                                                             |                   |                             |                          |
| **ECONOMY**                                                                 |                   |                             |                          |
| GDP (current US$)                                                           | NY.GDP.MKTP.CD    | Economy                     |           gdp            |
| Inflation, GDP deflator (annual %)                                          | NY.GDP.DEFL.KD.ZG | Economy                     |        inflation         |
| Tax revenue (% of GDP)                                                      | GC.TAX.TOTL.GD.ZS | Economy                     |       tax_revenue        |
Foi feita normalização nas entidades _Population_ e _Life Expectancy_, para separar dados em tabelas menores para reduzir redundância e dependências indesejadas. Portanto, nossa estruturação utiliza a **1FN (Primeira Forma Normal)** e **3FN (Terceira Forma Normal)**.

#### [Poverty_Inequality](https://github.com/FernandoRST7/Postgres-Database-Project-PovertyStudy/blob/main/processing/poverty_inequality/Poverty_Inequality_filtered.csv)

| Description                                                        | Field             |
| ------------------------------------------------------------------ | ----------------- |
| **SURVEY**                                                         |                   |
| Research institution acronym.                                      | survey_acronym    |
| Geographic/demographic coverage (e.g., urban, rural).              | survey_coverage   |
| Data aggregation level (e.g., national, regional).                 | reporting_level   |
| Welfare metric (income or consumption).                            | welfare_type      |
| Notes on comparability with other surveys.                         | comparability     |
| Time period for temporal comparisons.                              | comparable_spell  |
| Monetary poverty threshold (in USD).                               | poverty_line      |
| % of population below the poverty line.                            | headcount         |
| Average income shortfall of the poor relative to the poverty line. | poverty_gap       |
| Inequality among the poor (weights those furthest from the line).  | poverty_severity  |
| Gini coefficient (0 = perfect equality, 1 = maximum inequality).   | gini              |
| Total population covered in the survey.                            | reporting_pop     |
| Reported per capita consumption expenditure.                       | reporting_pce     |
| Data distribution method (e.g., nominal, adjusted).                | distribution_type |
| Secondary/specific poverty line (if applicable).                   | spl               |
|                                                                    |                   |
| **DECILE**                                                         |                   |
| Decile position ("Decile 1" to "Decile 10")                        | name              |
| % of population in the given decile.                               | value             |


![Conceptual_model](models/Conceptual_model.png)

### 2. Modelo Relacional

A transformação do modelo conceitual para o modelo relacional, especificando tabelas, atributos, chaves primárias e estrangeiras.

![Relational_model](models/Relational_model.png)

### 3. Modelo Físico

O script de criação (DDL) do banco de dados foi escrito em SQL padrão e inclui:
- Criação de tabelas
- Definição de chaves primárias e estrangeiras
- Restrições de integridade

### 4. Consultas SQL

Elaboramos pelo menos 5 consultas SQL não triviais, que:
- Integram dados de mais de uma tabela
- Fazem uso de agrupamentos (`GROUP BY`), ordenações (`ORDER BY`) e operações de junção (`JOIN`)
  

### 5. Implementação em Python

A implementação foi feita utilizando a biblioteca `sqlite3` para a conexão com o banco de dados e manipulação das operações.

## Autores

- [Fernando Rodrigues - 247409](https://github.com/FernandoRST7)
- [Victor Ogitsu - 244075](https://github.com/pancollenn)
- [Matheus Veiga - 269494](https://github.com/mvl18)
