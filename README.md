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
- Abaixo temos os indicadores escolhidos de cada dataset separado em suas entidades. Vale lembrar que a tabela abaixo tem apenas a parte dos indicadores, sendo que cada informação dessa possui seu país/região e ano de origem da pesquisa.
- Foi feita normalização nas entidades _Population_ e _Life Expectancy_, para separar dados em tabelas menores para reduzir redundância e dependências indesejadas. Portanto, nossa estruturação utiliza a 3FN (Terceira Forma Normal) e consequentemente a 1FN e 2FN.

#### [Global Indicators](https://github.com/FernandoRST7/Postgres-Database-Project-PovertyStudy/blob/main/processing/global_indicators/Global_Indicators_filtered.csv)

| INDICATOR                                                                   | INDICATOR CODE    | STRONG ENTITY / WEAK ENTITY |      ATTRIBUTE NAME      |
| --------------------------------------------------------------------------- | ----------------- | --------------------------- | :----------------------: |
| **DEMOGRAPHY**                                                              |                   |                             |                          |
| Population ages 0-14, female                                                | SP.POP.0014.FE.IN | Demography/Population       | pop_ages, gender, number |
| Population ages 0-14, male                                                  | SP.POP.0014.MA.IN | Demography/Population       |            -             |
| Population ages 15-64, female                                               | SP.POP.1564.FE.IN | Demography/Population       |            -             |
| Population ages 15-64, male                                                 | SP.POP.1564.MA.IN | Demography/Population       |            -             |
| Population ages 65 and above, female                                        | SP.POP.65UP.FE.IN | Demography/Population       |            -             |
| Population ages 65 and above, male                                          | SP.POP.65UP.MA.IN | Demography/Population       |            -             |
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

- Foi feita a transformação do modelo conceitual para o modelo relacional, especificando tabelas, atributos, chaves primárias e estrangeiras.
- Desta forma foi possível conseguir 

![Relational_model](models/Relational_model.png)

### 3. Modelo Físico

A partir do modelo relacional feito no ERD do pgAdmin, foi gerado o modelo físico do banco de dados, pronto para ser executado em um servidor PostgreSQL.

O script de criação (DDL) do banco de dados foi escrito em SQL padrão e inclui:
- Criação de tabelas
- Definição de chaves primárias e estrangeiras
- Restrições de integridade

Este modelo pode ser encontrado [aqui](models/Physical_model.sql).

### 4. Popular Banco de Dados

Para popular o banco de dados, criamos várias tabelas, uma para cada entidade do Banco de Dados. Os códigos para criação das entidades podem ser encontradas aqui([Global Indicators](processing/global_indicators/Global_Indicators_entity.py) e [Poverty and Inequality](processing/poverty_inequality/Poverty_Inequality_entity.py))

### 5. Consultas SQL

Elaboramos as seguintes consultas SQL avançadas, que integram dados de várias tabelas, utilizam agrupamentos, ordenações, operações de junção e funções analíticas para explorar correlações significativas entre os dados:

#### 1. Análise de Desemprego, Desigualdade e Educação por País [[RESULTADO]](python-SQL/Results/SQL%201.csv)
Consulta que relaciona as médias de taxa de desemprego, o coeficiente de Gini e a taxa de matrícula no ensino secundário para avaliar como a educação pode influenciar a desigualdade e o desemprego em diferentes países.
```sql
SELECT 
    c.country_name,
    ROUND(AVG(e.unemp)::numeric, 2) AS avg_unemployment_rate,
    ROUND(AVG(s.gini)::numeric, 2) AS avg_gini_coefficient,
    ROUND(AVG(ed.sec_enrol)::numeric, 2) AS avg_secondary_enrollment_rate,
    CASE 
        WHEN AVG(ed.sec_enrol) > 80 THEN 'High Enrollment'
        WHEN AVG(ed.sec_enrol) BETWEEN 50 AND 80 THEN 'Moderate Enrollment'
        ELSE 'Low Enrollment'
    END AS enrollment_category

FROM 
    employment e
JOIN 
    country c ON e.country_code = c.country_code
JOIN 
    survey s ON c.country_code = s.country_code
JOIN 
    education ed ON c.country_code = ed.country_code AND e.year = ed.year

WHERE 
    e.year = s.survey_year
    AND ed.sec_enrol IS NOT NULL
    AND s.gini IS NOT NULL

GROUP BY 
    c.country_name

ORDER BY 
    c.country_name;
```

#### 2. Expectativa de Vida, Despesas com Saúde e Taxa de Mortalidade [[RESULTADO]](python-SQL/Results/SQL%202.csv)
Consulta que relaciona a expectativa média de vida, os gastos médios com saúde e a taxa média de mortalidade, destacando países por gênero e agrupando-os em quartis de investimento em saúde, para identificar padrões entre investimento, longevidade e mortalidade.
```sql
SELECT 
    c.country_name,
    le.gender,
    ROUND(AVG(le.value)::numeric, 2) AS avg_life_expectancy,
    ROUND(AVG(h.expenditure)::numeric, 2) AS avg_health_expenditure,
    ROUND(AVG(dm.death_rate)::numeric, 2) AS avg_crude_death_rate,
    NTILE(4) OVER (PARTITION BY le.gender ORDER BY AVG(h.expenditure) DESC) AS health_expenditure_quartile,
    ROUND((AVG(le.value) / NULLIF(AVG(h.expenditure), 0))::numeric, 2) AS avg_life_expectancy_per_expenditure

FROM 
    life_expectancy le
JOIN 
    demography dm ON le.id_demography = dm.id_demography
JOIN 
    country c ON dm.id_country = c.id_country
JOIN 
    health h ON c.id_country = h.id_country AND dm.year = h.year

WHERE 
    le.value IS NOT NULL
    AND h.expenditure IS NOT NULL
    AND dm.death_rate IS NOT NULL
    AND h.expenditure > 0

GROUP BY 
    c.country_name,
    le.gender

ORDER BY 
    c.country_name,
    le.gender,
    health_expenditure_quartile, 
    avg_life_expectancy_per_expenditure DESC, 
    avg_crude_death_rate ASC;
```

#### 3. População Urbana, PIB e Taxa de Migração [[RESULTADO]](python-SQL/Results/SQL%203.csv)
Consulta que mostra, para cada país, o dado mais recente disponível sobre população urbana, PIB e taxa de migração líquida, destacando a taxa de urbanização e a tendência migratória (entrada, saída ou estabilidade populacional).
```sql
SELECT 
    c.country_name,
    d.urban_pop AS urban_population,
    ROUND(e.gdp::numeric, 5) AS gross_domestic_product,
    ROUND(d.net_migration::numeric, 3) AS net_migration,
    ROUND(((d.urban_pop / (d.urban_pop + d.rural_pop)) * 100)::numeric, 5) AS urbanization_rate,
    CASE 
        WHEN d.net_migration > 0 THEN 'Net Influx'
        WHEN d.net_migration < 0 THEN 'Net Outflux'
        ELSE 'Stable Migration'
    END AS migration_trend

FROM 
    demography d
JOIN 
    country c ON d.id_country = c.id_country
JOIN 
    economy e ON c.id_country = e.id_country AND d.year = e.year

WHERE 
    d.urban_pop IS NOT NULL
    AND d.rural_pop IS NOT NULL
    AND e.gdp IS NOT NULL

ORDER BY 
    urbanization_rate DESC, gross_domestic_product DESC;
```

#### 4. Impacto da Educação e Saúde na Redução da Pobreza [[RESULTADO]](python-SQL/Results/SQL%204.csv)
Consulta que avalia como a taxa média de matrícula no ensino primário e os gastos médios com saúde se relacionam com a taxa média de pobreza, criando um índice combinado de educação e saúde para analisar seu impacto na redução da pobreza.
```sql
SELECT 
    c.country_name,
    ROUND(AVG(ed.prim_enrol)::numeric, 10) AS avg_primary_enrollment_rate,
    ROUND(AVG(h.expenditure)::numeric, 10) AS avg_health_expenditure,
    ROUND(AVG(s.headcount)::numeric, 10) AS avg_poverty_rate,
    ROUND((AVG(ed.prim_enrol) * AVG(h.expenditure))::numeric, 10) AS education_health_index,
    CASE 
        WHEN AVG(s.headcount) * 100 < 10 THEN 'Low Poverty'
        WHEN AVG(s.headcount) * 100 BETWEEN 10 AND 30 THEN 'Moderate Poverty'
        ELSE 'High Poverty'
    END AS poverty_category

FROM 
    education ed
JOIN 
    country c ON ed.id_country = c.id_country
JOIN 
    health h ON c.id_country = h.id_country AND ed.year = h.year
JOIN 
    survey s ON c.id_country = s.id_country AND ed.year = s.survey_year

WHERE 
    ed.prim_enrol IS NOT NULL
    AND h.expenditure IS NOT NULL
    AND s.headcount IS NOT NULL

GROUP BY 
    c.country_name

ORDER BY 
    education_health_index DESC, avg_poverty_rate ASC;
```

#### 5. Desigualdade, Taxa de Pobreza e Distribuição de Renda [[RESULTADO]](python-SQL/Results/SQL%205.csv)
Consulta que, para a pesquisa mais recente de cada país, apresenta o coeficiente de Gini, a taxa de pobreza e a diferença entre a participação dos 10% mais ricos e dos 10% mais pobres na renda,classificando os países conforme o grau de desigualdade na distribuição de renda.
```sql
SELECT 
    c.country_name,
    s.survey_year,
    ROUND(s.gini::numeric, 3) AS gini_coefficient,
    ROUND((s.headcount * 100)::numeric, 2) AS poverty_rate_percent,
    d1.value AS decile_1_income_share,
    d10.value AS decile_10_income_share,
    CASE
		WHEN d1.value IS NULL OR d10.value IS NULL THEN NULL
		WHEN (d10.value - d1.value) < 0.18 THEN 'Low Decile Gap'
        WHEN (d10.value - d1.value) < 0.25 THEN 'Moderate Decile Gap'
        WHEN (d10.value - d1.value) < 0.35 THEN 'High Decile Gap'
        ELSE 'Very High Decile Gap'
    END AS decile_inequality_category

FROM 
    country c

JOIN (
    SELECT DISTINCT ON (id_country) *
    FROM survey
    ORDER BY id_country, survey_year DESC
) s ON c.id_country = s.id_country
LEFT JOIN decile d1 ON s.id_survey = d1.id_survey AND (d1.name = 'decile1')
LEFT JOIN decile d10 ON s.id_survey = d10.id_survey AND (d10.name = 'decile10')

ORDER BY 
    gini_coefficient DESC, poverty_rate_percent DESC;
```

Essas consultas foram projetadas para explorar correlações complexas entre os dados e fornecer insights detalhados alinhados aos Objetivos de Desenvolvimento Sustentável (ODS).

### 6. Implementação em Python

Todos os filtros foram feitos em python, assim como os sripts para popular o banco de dados e realizar as consultas SQL. Cada scypt em sua respectiva pasta no repositório.

## Autores

- [Fernando Rodrigues - 247409](https://github.com/FernandoRST7)
- [Victor Ogitsu - 244075](https://github.com/pancollenn)
- [Matheus Veiga - 269494](https://github.com/mvl18)
