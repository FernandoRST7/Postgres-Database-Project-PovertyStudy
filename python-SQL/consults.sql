--1. Análise de Desemprego, Desigualdade e Educação por País
--Consulta que relaciona a taxa de desemprego, o coeficiente de Gini e a taxa de matrícula no ensino
--secundário para avaliar como a educação pode influenciar a desigualdade e o desemprego em diferentes países.
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
    country c ON e.id_country = c.id_country
JOIN 
    survey s ON c.id_country = s.id_country
JOIN 
    education ed ON c.id_country = ed.id_country AND e.year = ed.year
WHERE 
    e.year = s.survey_year
    AND ed.sec_enrol IS NOT NULL
    AND s.gini IS NOT NULL
GROUP BY 
    c.country_name
ORDER BY 
    c.country_name;

--2. Expectativa de Vida, Gastos com Saúde e Taxa de Mortalidade
--Consulta que relaciona a expectativa média de vida, os gastos médios com saúde e a taxa média de mortalidade,
--destacando países por gênero e agrupando-os em quartis de investimento em saúde, para identificar padrões entre
--investimento, longevidade e mortalidade.
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


--3. População Urbana, PIB e Migração Líquida
--Consulta que mostra, para cada país, o dado mais recente disponível sobre população urbana, PIB e taxa de migração
--líquida, destacando a taxa de urbanização e a tendência migratória (entrada, saída ou estabilidade populacional).
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


--4. Impacto da Educação e Saúde na Pobreza
--Consulta que avalia como a taxa média de matrícula no ensino primário e os gastos médios com saúde se relacionam com a
--taxa média de pobreza, criando um índice combinado de educação e saúde para analisar seu impacto na redução da pobreza.
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

--5. Desigualdade, Pobreza e Distribuição de Renda por Decil
--Consulta que, para a pesquisa mais recente de cada país, apresenta o coeficiente de Gini, a taxa de pobreza e 
--a diferença entre a participação dos 10% mais ricos e dos 10% mais pobres na renda,classificando os países
--conforme o grau de desigualdade na distribuição de renda.
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