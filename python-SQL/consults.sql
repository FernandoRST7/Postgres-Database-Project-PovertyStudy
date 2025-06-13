--1. Análise de Desemprego, Desigualdade e Educação por País
--Consulta que relaciona a taxa de desemprego, o coeficiente de Gini 
--e a taxa de matrícula no ensino secundário para avaliar como a educação 
--pode influenciar a desigualdade e o desemprego em diferentes países.
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
    country c ON e.id_country = c.id_country -- ATUALIZADO
JOIN 
    survey s ON c.id_country = s.id_country -- ATUALIZADO
JOIN 
    education ed ON c.id_country = ed.id_country AND e.year = ed.year -- ATUALIZADO
WHERE 
    e.year = s.survey_year
    AND ed.sec_enrol IS NOT NULL
    AND s.gini IS NOT NULL
GROUP BY 
    c.country_name
ORDER BY 
    c.country_name;

-- 2. Expectativa de Vida, Despesas com Saúde e Taxa de Mortalidade
-- Consulta que analisa a relação entre a expectativa de vida, os gastos 
-- com saúde como percentual do PIB e a taxa de mortalidade, destacando países 
-- com altos gastos em saúde, mas baixa expectativa de vida.
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
    country c ON dm.id_country = c.id_country -- ATUALIZADO
JOIN 
    health h ON c.id_country = h.id_country AND dm.year = h.year -- ATUALIZADO
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

-- 3. População Urbana, PIB e Taxa de Migração
-- Consulta que verifica a relação entre a população urbana, 
-- o PIB e a taxa de migração líquida, destacando países com alta urbanização e crescimento econômico.
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
    country c ON d.id_country = c.id_country -- ATUALIZADO
JOIN 
    economy e ON c.id_country = e.id_country AND d.year = e.year -- ATUALIZADO
WHERE 
    d.urban_pop IS NOT NULL
    AND d.rural_pop IS NOT NULL
    AND e.gdp IS NOT NULL
ORDER BY 
    urbanization_rate DESC, gross_domestic_product DESC;

--4. Impacto da Educação e Saúde na Redução da Pobreza
--Consulta que relaciona a taxa de matrícula no ensino primário, 
--os gastos com saúde e a porcentagem da população abaixo da linha de pobreza 
--para avaliar o impacto combinado da educação e saúde na redução da pobreza.
SELECT 
    c.country_name,
    ROUND(AVG(ed.prim_enrol)::numeric, 10) AS avg_primary_enrollment_rate,
    ROUND(AVG(h.expenditure)::numeric, 10) AS avg_health_expenditure,
    ROUND(AVG(s.headcount)::numeric, 10) AS avg_poverty_rate,
    ROUND((AVG(ed.prim_enrol) * AVG(h.expenditure))::numeric, 10) AS education_health_index,
    CASE 
        WHEN AVG(s.headcount) < 10 THEN 'Low Poverty'
        WHEN AVG(s.headcount) BETWEEN 10 AND 30 THEN 'Moderate Poverty'
        ELSE 'High Poverty'
    END AS poverty_category
FROM 
    education ed
JOIN 
    country c ON ed.id_country = c.id_country -- ATUALIZADO
JOIN 
    health h ON c.id_country = h.id_country AND ed.year = h.year -- ATUALIZADO
JOIN 
    survey s ON c.id_country = s.id_country AND ed.year = s.survey_year -- ATUALIZADO
WHERE 
    ed.prim_enrol IS NOT NULL
    AND h.expenditure IS NOT NULL
    AND s.headcount IS NOT NULL
GROUP BY 
    c.country_name
ORDER BY 
    education_health_index DESC, avg_poverty_rate ASC;

--5. Desigualdade, Taxa de Pobreza e Distribuição de Renda
--Consulta que analisa a relação entre o coeficiente de Gini, 
--a taxa de pobreza e a distribuição de renda por decil, 
--destacando países com alta desigualdade e pobreza.
SELECT 
    c.country_name,
    ROUND(s.gini::numeric, 5) AS gini_coefficient,
    ROUND(s.headcount::numeric, 5) AS poverty_rate,
    d.name AS income_decile,
    ROUND(d.value::numeric, 5) AS income_share,
    ROUND(SUM(d.value) OVER (PARTITION BY c.country_name ORDER BY d.name)::numeric, 5) AS cumulative_income_share,
    CASE 
        WHEN s.gini > 0.4 THEN 'High Inequality'
        ELSE 'Low Inequality'
    END AS inequality_category
FROM 
    survey s
JOIN 
    country c ON s.id_country = c.id_country -- ATUALIZADO
JOIN 
    decile d ON s.id_survey = d.id_survey
WHERE 
    s.gini IS NOT NULL
    AND s.headcount IS NOT NULL
ORDER BY 
    gini_coefficient DESC, poverty_rate DESC, cumulative_income_share ASC;