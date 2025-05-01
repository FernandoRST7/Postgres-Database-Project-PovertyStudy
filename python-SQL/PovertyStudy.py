import psycopg2
import pandas as pd

# Conecta com o banco de dados
conn = psycopg2.connect(
    dbname='postgres',
    user='postgres',
    password='1001',
    host='localhost',
    port='5432'
)

# Cria o cursor
cursor = conn.cursor()

# Cria tabela no db
try:
    cursor.execute("""
                
-- Drop existing tables if they exist (optional, for clean runs)
-- Use with caution, this will delete all data!
DROP TABLE IF EXISTS COUNTRY CASCADE;
DROP TABLE IF EXISTS SURVEY CASCADE;
DROP TABLE IF EXISTS REGION CASCADE; -- Typo matches schema
DROP TABLE IF EXISTS DEMOGRAPHY CASCADE;
DROP TABLE IF EXISTS HEALTH CASCADE;
DROP TABLE IF EXISTS EDUCATION CASCADE;
DROP TABLE IF EXISTS ECONOMY CASCADE;
DROP TABLE IF EXISTS POPULATION CASCADE;
DROP TABLE IF EXISTS EMPLOYMENT CASCADE;
DROP TABLE IF EXISTS LIFE_EXPECTANCY CASCADE;
DROP TABLE IF EXISTS DECILE CASCADE;


CREATE TABLE IF NOT EXISTS public.country
(
    country_code character varying NOT NULL,
    country_name character varying,
    region_code character varying NOT NULL,
    PRIMARY KEY (country_code)
);

CREATE TABLE IF NOT EXISTS public.survey
(
    id_survey serial NOT NULL,
    country_code character varying NOT NULL,
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

CREATE TABLE IF NOT EXISTS public.region
(
    region_code character varying NOT NULL,
    region_name character varying NOT NULL,
    PRIMARY KEY (region_code)
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
    country_code character varying NOT NULL,
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
    country_code character varying NOT NULL,
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
    country_code character varying NOT NULL,
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
    country_code character varying NOT NULL,
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
    country_code character varying NOT NULL,
    year integer,
    hospital_beds double precision,
    physicians double precision,
    expenditure double precision,
    PRIMARY KEY (id_health)
);

ALTER TABLE IF EXISTS public.country
    ADD FOREIGN KEY (region_code)
    REFERENCES public.region (region_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.survey
    ADD FOREIGN KEY (country_code)
    REFERENCES public.country (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.decile
    ADD FOREIGN KEY (id_survey)
    REFERENCES public.survey (id_survey) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.demography
    ADD FOREIGN KEY (country_code)
    REFERENCES public.country (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.employment
    ADD FOREIGN KEY (country_code)
    REFERENCES public.country (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.education
    ADD FOREIGN KEY (country_code)
    REFERENCES public.country (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.economy
    ADD FOREIGN KEY (country_code)
    REFERENCES public.country (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.population
    ADD FOREIGN KEY (id_demography)
    REFERENCES public.demography (id_demography) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.life_expectancy
    ADD FOREIGN KEY (id_demography)
    REFERENCES public.demography (id_demography) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public.health
    ADD FOREIGN KEY (country_code)
    REFERENCES public.country (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
    """)

    conn.commit()
    print("Tabela recriada do zero!")

except Exception as e:
    conn.rollback()
    print(f"Erro: {e}") 

cursor.execute("SET session_replication_role = 'origin';")
conn.commit()

# Lê o CSV e separa os dados
df_poverty = pd.read_csv('processing\poverty_inequality\Poverty_Inequality_filtered.csv')
df_indicators = pd.read_csv('processing\global_indicators\Global_Indicators_filtered.csv')

df_region = pd.read_csv('processing\poverty_inequality\entities_csv\Region.csv')
df_country = pd.read_csv('processing\poverty_inequality\entities_csv\Country.csv')


df_survey = df_poverty[['country_code', 'welfare_type', 'survey_acronym', 'survey_comparability', 
                'comparable_spell', 'poverty_line', 'headcount', 'poverty_gap', 
                'poverty_severity', 'gini', 'reporting_pop', 'reporting_pce', 
                'distribution_type', 'spl', 'survey_year']]  # Para a tabela 'Survey'


# Passo 1: Carregar dados na tabela REGION
# Verificar duplicidade e só inserir valores únicos
df_region.drop_duplicates(subset=['region_code'], inplace=True)  # Remover duplicatas por 'region_code'


with open('processing\poverty_inequality\entities_csv\Region.csv', 'r') as f:
    cursor.copy_expert("""
        COPY REGION (region_code, region_name)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
    """, f)
conn.commit()
print("Tabela Region populada com sucesso!")


# Passo 2: Carregar dados na tabela COUNTRY
with open('processing\poverty_inequality\entities_csv\Country.csv', 'r') as f1: # Pode dar erro com duplicatas
    cursor.copy_expert("""
        COPY COUNTRY (region_code, country_name, country_code)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
    """, f1)
conn.commit()
print("Tabela Country populada com sucesso!")


# Passo 3: Carregar dados na tabela SURVEY
with open('processing\poverty_inequality\entities_csv\Survey.csv', 'r') as f2:
    cursor.copy_expert("""
        COPY SURVEY(country_code, welfare_type, survey_acronym, survey_comparability, 
                               comparable_spell, poverty_line, headcount, poverty_gap, poverty_severity, 
                               gini, reporting_pop, reporting_pce, distribution_type, spl, survey_year, survey_coverage, reporting_level)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',')
    """, f2)
conn.commit()
print("Tabela Survey populada com sucesso!")


# Passo 4: Carregar dados na tabela DECILE
# Criar uma tabela temporária no banco de dados
cursor.execute("""
    CREATE TEMP TABLE temp_decile (
        country_code VARCHAR,
        survey_year INTEGER,
        survey_acronym VARCHAR,
        survey_coverage VARCHAR,
        reporting_level VARCHAR,
        name VARCHAR,
        value DOUBLE PRECISION
    );
""")
conn.commit()

# Carregar o CSV de Decile
df_decile = pd.read_csv("processing\poverty_inequality\entities_csv\Decile.csv")
 
# Inserir os dados do DataFrame na tabela temporária
for _, row in df_decile.iterrows():
    cursor.execute("""
        INSERT INTO temp_decile (country_code, survey_year, survey_acronym, survey_coverage, reporting_level, name, value)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """, (row['country_code'], row['survey_year'], row['survey_acronym'], 
          row['survey_coverage'], row['reporting_level'], row['name'], row['value']))
conn.commit()

# Inserir os dados na tabela DECILE
cursor.execute("""
    INSERT INTO DECILE (name, value, id_survey)
    SELECT
        t.name,
        t.value,
        s.id_survey
    FROM
        temp_decile t
    INNER JOIN
        SURVEY s
    ON
        t.country_code = s.country_code AND 
        t.survey_year = s.survey_year AND 
        t.survey_acronym = s.survey_acronym AND
        t.survey_coverage = s.survey_coverage AND 
        t.reporting_level = s.reporting_level;
""")
conn.commit()

# Remover a tabela temporária
cursor.execute("DROP TABLE temp_decile;")
conn.commit()
print("Tabela Decile populada com sucesso!")


# Passo 5: Carregar dados na tabela ECONOMY
with open('processing\global_indicators\entities_csv\Economy.csv', 'r') as f2:
    cursor.copy_expert("""
        COPY ECONOMY(year, country_code, tax_revenue, inflation, gdp)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',')
    """, f2)
conn.commit()
print("Tabela Economy populada com sucesso!")


# Passo 6: Carregar dados na tabela EMPLOYMENT
with open('processing\global_indicators\entities_csv\Employment.csv', 'r') as f2:
    cursor.copy_expert("""
        COPY EMPLOYMENT(year, country_code, child_emp, unemp, vulnerable_emp, part_time, employers, labor_force_total, labor_force_fem)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',')
    """, f2)
conn.commit()
print("Tabela Employment populada com sucesso!")


# passo 7: Carregar dados na tabela EDUCATION
with open('processing\global_indicators\entities_csv\Education.csv', 'r') as f2:
    cursor.copy_expert("""
        COPY EDUCATION(year, country_code, child_out_of_school, progression_to_sec, expenditure, preprim_enroll, prim_enrol, sec_enrol, terti_enrol)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',')
    """, f2)
conn.commit()
print("Tabela Education populada com sucesso!")


# Passo 8: Carregar dados na tabela HEALTH
with open('processing\global_indicators\entities_csv\Health.csv', 'r') as f2:
    cursor.copy_expert("""
        COPY HEALTH(year, country_code, hospital_beds, physicians, expenditure)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',')
    """, f2)
conn.commit()
print("Tabela Health populada com sucesso!")


# Passo 9: Carregar dados na tabela DEMOGRAPHY
with open('processing\global_indicators\entities_csv\Demography.csv', 'r') as f2:
    cursor.copy_expert("""
        COPY DEMOGRAPHY(year, country_code, pop_density, urban_pop, rural_pop, net_migration, death_rate, birth_rate)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',')
    """, f2)
conn.commit()
print("Tabela Demography populada com sucesso!")


# Psso 10: Carregar dados na tabela POPULATION (similar ao de decile\Passo 4)
# Criar uma tabela temporária no banco de dados
cursor.execute("""
    CREATE TEMP TABLE temp_population (
        year INTEGER,
        country_code VARCHAR,
        pop_ages VARCHAR,
        gender VARCHAR,
        number DOUBLE PRECISION
    );
""")
conn.commit()

# Carregar o CSV de Population
df_population = pd.read_csv("processing\global_indicators\entities_csv\Population.csv")

# Inserir os dados do DataFrame na tabela temporária
for _, row in df_population.iterrows():
    cursor.execute("""
        INSERT INTO temp_population (year, country_code, pop_ages, gender, number)
        VALUES (%s, %s, %s, %s, %s);
    """, (row['year'], row['country_code'], row['pop_ages'], row['gender'], row['number']))
conn.commit()

# Inserir os dados na tabela Population com id_demography
cursor.execute("""
    INSERT INTO POPULATION (id_demography, pop_ages, gender, number)
    SELECT
        d.id_demography,
        t.pop_ages,
        t.gender,
        t.number
    FROM
        temp_population t
    INNER JOIN
        DEMOGRAPHY d
    ON
        t.year = d.year AND t.country_code = d.country_code;
""")
conn.commit()

# Remover a tabela temporária
cursor.execute("DROP TABLE temp_population;")
conn.commit()

print("Tabela Population populada com sucesso!")


# Passo 11: Carregar dados na tabela LIFE_EXPECTANCY
# Criar uma tabela temporária no banco de dados
cursor.execute("""
    CREATE TEMP TABLE temp_life_expectancy (
        year INTEGER,
        country_code VARCHAR,
        gender VARCHAR,
        value DOUBLE PRECISION
    );
""")
conn.commit()

# Carregar o CSV de Life Expectancy
df_life_expectancy = pd.read_csv("processing\global_indicators\entities_csv\Life_expectancy.csv")

# Inserir os dados do DataFrame na tabela temporária
for _, row in df_life_expectancy.iterrows():
    cursor.execute("""
        INSERT INTO temp_life_expectancy (year, country_code, gender, value)
        VALUES (%s, %s, %s, %s);
    """, (row['year'], row['country_code'], row['gender'], row['value']))
conn.commit()

# Inserir os dados na tabela LIFE_EXPECTANCY com id_demography
cursor.execute("""
    INSERT INTO LIFE_EXPECTANCY (id_demography, gender, value)
    SELECT
        d.id_demography,
        t.gender,
        t.value
    FROM
        temp_life_expectancy t
    INNER JOIN
        DEMOGRAPHY d
    ON
        t.year = d.year AND t.country_code = d.country_code;
""")
conn.commit()

# Remover a tabela temporária
cursor.execute("DROP TABLE temp_life_expectancy;")
conn.commit()

print("Tabela Life Expectancy populada com sucesso!")

# Commit FINAL das inserções
conn.commit()

# Fechar a conexão
cursor.close()
conn.close()