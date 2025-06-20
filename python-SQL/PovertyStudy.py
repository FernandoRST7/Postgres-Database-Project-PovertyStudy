import psycopg2
import pandas as pd

# Conecta com o banco de dados
conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password=,  # Senha do usuário postgres
    host='localhost',
    port='5432'
)

# Cria o cursor
cursor = conn.cursor()

# Cria tabela no db com o NOVO MODELO
try:
    cursor.execute("""
-- BEGIN transaction block
BEGIN;

-- Drop existing tables if they exist (for clean runs)
DROP TABLE IF EXISTS public.country CASCADE;
DROP TABLE IF EXISTS public.survey CASCADE;
DROP TABLE IF EXISTS public.region CASCADE;
DROP TABLE IF EXISTS public.demography CASCADE;
DROP TABLE IF EXISTS public.health CASCADE;
DROP TABLE IF EXISTS public.education CASCADE;
DROP TABLE IF EXISTS public.economy CASCADE;
DROP TABLE IF EXISTS public.population CASCADE;
DROP TABLE IF EXISTS public.employment CASCADE;
DROP TABLE IF EXISTS public.life_expectancy CASCADE;
DROP TABLE IF EXISTS public.decile CASCADE;

-- Create tables based on the new schema
CREATE TABLE IF NOT EXISTS public.region
(
    id_region serial NOT NULL,
    region_code character varying NOT NULL,
    region_name character varying NOT NULL,
    PRIMARY KEY (id_region),
    UNIQUE (region_code) -- Add unique constraint for joining
);

CREATE TABLE IF NOT EXISTS public.country
(
    id_country serial NOT NULL,
    country_code character varying NOT NULL,
    country_name character varying,
    id_region integer NOT NULL,
    PRIMARY KEY (id_country),
    UNIQUE (country_code) -- Add unique constraint for joining
);

CREATE TABLE IF NOT EXISTS public.survey
(
    id_survey serial NOT NULL,
    id_country integer NOT NULL,
    welfare_type character varying,
    survey_acronym character varying,
    survey_comparability integer,
    comparable_spell character varying,
    poverty_line double precision,
    headcount double precision,
    poverty_gap double precision,
    poverty_severity double precision,
    gini double precision,
    reporting_pop integer,
    reporting_pce double precision,
    distribution_type character varying,
    spl double precision,
    survey_year integer,
    survey_coverage character varying,
    reporting_level character varying,
    PRIMARY KEY (id_survey)
);

CREATE TABLE IF NOT EXISTS public.decile
(
    id_decile serial NOT NULL,
    value double precision,
    name character varying,
    id_survey integer NOT NULL,
    PRIMARY KEY (id_decile)
);

CREATE TABLE IF NOT EXISTS public.demography
(
    id_demography serial NOT NULL,
    year integer,
    id_country integer NOT NULL,
    pop_density double precision,
    urban_pop double precision,
    rural_pop double precision,
    net_migration double precision,
    death_rate double precision,
    birth_rate double precision,
    PRIMARY KEY (id_demography)
);

CREATE TABLE IF NOT EXISTS public.employment
(
    id_employment serial NOT NULL,
    year integer,
    id_country integer NOT NULL,
    child_emp double precision,
    unemp double precision,
    vulnerable_emp double precision,
    part_time double precision,
    employers double precision,
    labor_force_total double precision,
    labor_force_fem double precision,
    PRIMARY KEY (id_employment)
);

CREATE TABLE IF NOT EXISTS public.education
(
    id_education serial NOT NULL,
    year integer,
    id_country integer NOT NULL,
    child_out_of_school double precision,
    progression_to_sec double precision,
    expenditure double precision,
    preprim_enroll double precision,
    prim_enrol double precision,
    sec_enrol double precision,
    terti_enrol double precision,
    PRIMARY KEY (id_education)
);

CREATE TABLE IF NOT EXISTS public.economy
(
    id_economy serial NOT NULL,
    year integer,
    id_country integer NOT NULL,
    gdp double precision,
    inflation double precision,
    tax_revenue double precision,
    PRIMARY KEY (id_economy)
);

CREATE TABLE IF NOT EXISTS public.population
(
    id_population serial NOT NULL,
    id_demography integer NOT NULL,
    pop_ages character varying,
    gender character varying,
    "number" double precision,
    PRIMARY KEY (id_population)
);

CREATE TABLE IF NOT EXISTS public.life_expectancy
(
    id_life_expectancy serial NOT NULL,
    id_demography integer NOT NULL,
    gender character varying,
    value double precision,
    PRIMARY KEY (id_life_expectancy)
);

CREATE TABLE IF NOT EXISTS public.health
(
    id_health serial NOT NULL,
    id_country integer NOT NULL,
    year integer,
    hospital_beds double precision,
    physicians double precision,
    expenditure double precision,
    PRIMARY KEY (id_health)
);

-- Add Foreign Keys
ALTER TABLE IF EXISTS public.country
    ADD FOREIGN KEY (id_region) REFERENCES public.region (id_region);
ALTER TABLE IF EXISTS public.survey
    ADD FOREIGN KEY (id_country) REFERENCES public.country (id_country);
ALTER TABLE IF EXISTS public.decile
    ADD FOREIGN KEY (id_survey) REFERENCES public.survey (id_survey);
ALTER TABLE IF EXISTS public.demography
    ADD FOREIGN KEY (id_country) REFERENCES public.country (id_country);
ALTER TABLE IF EXISTS public.employment
    ADD FOREIGN KEY (id_country) REFERENCES public.country (id_country);
ALTER TABLE IF EXISTS public.education
    ADD FOREIGN KEY (id_country) REFERENCES public.country (id_country);
ALTER TABLE IF EXISTS public.economy
    ADD FOREIGN KEY (id_country) REFERENCES public.country (id_country);
ALTER TABLE IF EXISTS public.health
    ADD FOREIGN KEY (id_country) REFERENCES public.country (id_country);
ALTER TABLE IF EXISTS public.population
    ADD FOREIGN KEY (id_demography) REFERENCES public.demography (id_demography);
ALTER TABLE IF EXISTS public.life_expectancy
    ADD FOREIGN KEY (id_demography) REFERENCES public.demography (id_demography);

COMMIT;
-- END transaction block
    """)
    conn.commit()
    print("Tabelas recriadas com o novo modelo!")

except Exception as e:
    conn.rollback()
    print(f"Erro ao recriar tabelas: {e}")

# --- INÍCIO DA CARGA DE DADOS ---

# Passo 1: Carregar dados na tabela REGION
with open('processing/poverty_inequality/entities_csv/Region.csv', 'r') as f:
    # id_region será gerado automaticamente pelo 'serial'
    cursor.copy_expert("""
        COPY REGION (region_code, region_name)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
    """, f)
conn.commit()
print("Tabela Region populada com sucesso!")

# Passo 2: Carregar dados na tabela COUNTRY (usando tabela temporária)
cursor.execute("CREATE TEMP TABLE temp_country (region_code VARCHAR, country_name VARCHAR, country_code VARCHAR);")
with open('processing/poverty_inequality/entities_csv/Country.csv', 'r') as f:
    cursor.copy_expert("COPY temp_country FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO COUNTRY (country_code, country_name, id_region)
    SELECT t.country_code, t.country_name, r.id_region
    FROM temp_country t
    JOIN REGION r ON t.region_code = r.region_code;
""")
cursor.execute("DROP TABLE temp_country;")
conn.commit()
print("Tabela Country populada com sucesso!")

# Passo 3: Carregar dados na tabela SURVEY (usando tabela temporária)
cursor.execute("""
    CREATE TEMP TABLE temp_survey (
        country_code VARCHAR, welfare_type VARCHAR, survey_acronym VARCHAR, survey_comparability INTEGER,
        comparable_spell VARCHAR, poverty_line DOUBLE PRECISION, headcount DOUBLE PRECISION, poverty_gap DOUBLE PRECISION,
        poverty_severity DOUBLE PRECISION, gini DOUBLE PRECISION, reporting_pop INTEGER, reporting_pce DOUBLE PRECISION,
        distribution_type VARCHAR, spl DOUBLE PRECISION, survey_year INTEGER, survey_coverage VARCHAR, reporting_level VARCHAR
    );
""")
with open('processing/poverty_inequality/entities_csv/Survey.csv', 'r') as f:
    cursor.copy_expert("COPY temp_survey FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO SURVEY (id_country, welfare_type, survey_acronym, survey_comparability, comparable_spell, poverty_line, headcount,
                        poverty_gap, poverty_severity, gini, reporting_pop, reporting_pce, distribution_type, spl, survey_year,
                        survey_coverage, reporting_level)
    SELECT c.id_country, t.welfare_type, t.survey_acronym, t.survey_comparability, t.comparable_spell, t.poverty_line, t.headcount,
           t.poverty_gap, t.poverty_severity, t.gini, t.reporting_pop, t.reporting_pce, t.distribution_type, t.spl, t.survey_year,
           t.survey_coverage, t.reporting_level
    FROM temp_survey t
    JOIN COUNTRY c ON t.country_code = c.country_code;
""")
cursor.execute("DROP TABLE temp_survey;")
conn.commit()
print("Tabela Survey populada com sucesso!")

# Passo 4: Carregar dados na tabela DECILE
cursor.execute("""
    CREATE TEMP TABLE temp_decile (
        country_code VARCHAR, survey_year INTEGER, survey_acronym VARCHAR, survey_coverage VARCHAR,
        reporting_level VARCHAR, name VARCHAR, value DOUBLE PRECISION
    );
""")
with open("processing/poverty_inequality/entities_csv/Decile.csv", 'r') as f:
     cursor.copy_expert("COPY temp_decile FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO DECILE (name, value, id_survey)
    SELECT t.name, t.value, s.id_survey
    FROM temp_decile t
    JOIN SURVEY s ON t.country_code = (SELECT country_code FROM country WHERE id_country = s.id_country)
                  AND t.survey_year = s.survey_year
                  AND t.survey_acronym = s.survey_acronym
                  AND t.survey_coverage = s.survey_coverage
                  AND t.reporting_level = s.reporting_level;
""")
cursor.execute("DROP TABLE temp_decile;")
conn.commit()
print("Tabela Decile populada com sucesso!")

# Passo 5: Carregar dados na tabela DEMOGRAPHY
cursor.execute("""
    CREATE TEMP TABLE temp_demography (
        year INTEGER, country_code VARCHAR, pop_density DOUBLE PRECISION, urban_pop DOUBLE PRECISION,
        rural_pop DOUBLE PRECISION, net_migration DOUBLE PRECISION, death_rate DOUBLE PRECISION, birth_rate DOUBLE PRECISION
    );
""")
with open('processing/global_indicators/entities_csv/Demography.csv', 'r') as f:
    cursor.copy_expert("COPY temp_demography FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO DEMOGRAPHY (year, id_country, pop_density, urban_pop, rural_pop, net_migration, death_rate, birth_rate)
    SELECT t.year, c.id_country, t.pop_density, t.urban_pop, t.rural_pop, t.net_migration, t.death_rate, t.birth_rate
    FROM temp_demography t
    JOIN COUNTRY c ON t.country_code = c.country_code;
""")
cursor.execute("DROP TABLE temp_demography;")
conn.commit()
print("Tabela Demography populada com sucesso!")

# Passo 6: Carregar dados na tabela ECONOMY
cursor.execute("CREATE TEMP TABLE temp_economy (year INTEGER, country_code VARCHAR, tax_revenue DOUBLE PRECISION, inflation DOUBLE PRECISION, gdp DOUBLE PRECISION);")
with open('processing/global_indicators/entities_csv/Economy.csv', 'r') as f:
    cursor.copy_expert("COPY temp_economy FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO ECONOMY (year, id_country, tax_revenue, inflation, gdp)
    SELECT t.year, c.id_country, t.tax_revenue, t.inflation, t.gdp
    FROM temp_economy t
    JOIN COUNTRY c ON t.country_code = c.country_code;
""")
cursor.execute("DROP TABLE temp_economy;")
conn.commit()
print("Tabela Economy populada com sucesso!")

# Passo 7: Carregar dados na tabela EMPLOYMENT
cursor.execute("""
    CREATE TEMP TABLE temp_employment (
        year INTEGER, country_code VARCHAR, child_emp DOUBLE PRECISION, unemp DOUBLE PRECISION, vulnerable_emp DOUBLE PRECISION,
        part_time DOUBLE PRECISION, employers DOUBLE PRECISION, labor_force_total DOUBLE PRECISION, labor_force_fem DOUBLE PRECISION
    );
""")
with open('processing/global_indicators/entities_csv/Employment.csv', 'r') as f:
    cursor.copy_expert("COPY temp_employment FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO EMPLOYMENT (year, id_country, child_emp, unemp, vulnerable_emp, part_time, employers, labor_force_total, labor_force_fem)
    SELECT t.year, c.id_country, t.child_emp, t.unemp, t.vulnerable_emp, t.part_time, t.employers, t.labor_force_total, t.labor_force_fem
    FROM temp_employment t
    JOIN COUNTRY c ON t.country_code = c.country_code;
""")
cursor.execute("DROP TABLE temp_employment;")
conn.commit()
print("Tabela Employment populada com sucesso!")

# Passo 8: Carregar dados na tabela EDUCATION
cursor.execute("""
    CREATE TEMP TABLE temp_education (
        year INTEGER, country_code VARCHAR, child_out_of_school DOUBLE PRECISION, progression_to_sec DOUBLE PRECISION,
        expenditure DOUBLE PRECISION, preprim_enroll DOUBLE PRECISION, prim_enrol DOUBLE PRECISION, sec_enrol DOUBLE PRECISION, terti_enrol DOUBLE PRECISION
    );
""")
with open('processing/global_indicators/entities_csv/Education.csv', 'r') as f:
    cursor.copy_expert("COPY temp_education FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO EDUCATION (year, id_country, child_out_of_school, progression_to_sec, expenditure, preprim_enroll, prim_enrol, sec_enrol, terti_enrol)
    SELECT t.year, c.id_country, t.child_out_of_school, t.progression_to_sec, t.expenditure, t.preprim_enroll, t.prim_enrol, t.sec_enrol, t.terti_enrol
    FROM temp_education t
    JOIN COUNTRY c ON t.country_code = c.country_code;
""")
cursor.execute("DROP TABLE temp_education;")
conn.commit()
print("Tabela Education populada com sucesso!")

# Passo 9: Carregar dados na tabela HEALTH
cursor.execute("CREATE TEMP TABLE temp_health (year INTEGER, country_code VARCHAR, hospital_beds DOUBLE PRECISION, physicians DOUBLE PRECISION, expenditure DOUBLE PRECISION);")
with open('processing/global_indicators/entities_csv/Health.csv', 'r') as f:
    cursor.copy_expert("COPY temp_health FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO HEALTH (year, id_country, hospital_beds, physicians, expenditure)
    SELECT t.year, c.id_country, t.hospital_beds, t.physicians, t.expenditure
    FROM temp_health t
    JOIN COUNTRY c ON t.country_code = c.country_code;
""")
cursor.execute("DROP TABLE temp_health;")
conn.commit()
print("Tabela Health populada com sucesso!")

# Passo 10: Carregar dados na tabela POPULATION (lógica original ainda é válida)
cursor.execute("CREATE TEMP TABLE temp_population (year INTEGER, country_code VARCHAR, pop_ages VARCHAR, gender VARCHAR, number DOUBLE PRECISION);")
with open("processing/global_indicators/entities_csv/Population.csv", 'r') as f:
    cursor.copy_expert("COPY temp_population FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO POPULATION (id_demography, pop_ages, gender, number)
    SELECT d.id_demography, t.pop_ages, t.gender, t.number
    FROM temp_population t
    JOIN COUNTRY c ON t.country_code = c.country_code
    JOIN DEMOGRAPHY d ON c.id_country = d.id_country AND t.year = d.year;
""")
cursor.execute("DROP TABLE temp_population;")
conn.commit()
print("Tabela Population populada com sucesso!")

# Passo 11: Carregar dados na tabela LIFE_EXPECTANCY (lógica original ainda é válida)
cursor.execute("CREATE TEMP TABLE temp_life_expectancy (year INTEGER, country_code VARCHAR, gender VARCHAR, value DOUBLE PRECISION);")
with open("processing/global_indicators/entities_csv/Life_expectancy.csv", 'r') as f:
    cursor.copy_expert("COPY temp_life_expectancy FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',');", f)
cursor.execute("""
    INSERT INTO LIFE_EXPECTANCY (id_demography, gender, value)
    SELECT d.id_demography, t.gender, t.value
    FROM temp_life_expectancy t
    JOIN COUNTRY c ON t.country_code = c.country_code
    JOIN DEMOGRAPHY d ON c.id_country = d.id_country AND t.year = d.year;
""")
cursor.execute("DROP TABLE temp_life_expectancy;")
conn.commit()
print("Tabela Life Expectancy populada com sucesso!")

# Fechar a conexão
cursor.close()
conn.close()

print("\nProcesso de carga de dados concluído com sucesso!")