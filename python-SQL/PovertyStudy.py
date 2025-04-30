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
CREATE TABLE IF NOT EXISTS public."Country"
(
    country_code character varying NOT NULL,
    country_name character varying,
    region_code character varying NOT NULL,
    PRIMARY KEY (country_code)
);

CREATE TABLE IF NOT EXISTS public."Survey"
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
    PRIMARY KEY (id_survey)
);

CREATE TABLE IF NOT EXISTS public."Region"
(
    region_code character varying NOT NULL,
    region_name character varying NOT NULL,
    PRIMARY KEY (region_code)
);

CREATE TABLE IF NOT EXISTS public."Decile"
(
    id_decile serial NOT NULL,
    value double precision,
    name character varying,
    id_survey integer NOT NULL,
    PRIMARY KEY (id_decile)
);

CREATE TABLE IF NOT EXISTS public."Demography"
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

CREATE TABLE IF NOT EXISTS public."Employment"
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

CREATE TABLE IF NOT EXISTS public."Education"
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

CREATE TABLE IF NOT EXISTS public."Economy"
(
    id_economy serial NOT NULL,
    year integer,
    country_code character varying NOT NULL,
    gdp double precision,
    inflation double precision,
    tax_revenue double precision,
    PRIMARY KEY (id_economy)
);

CREATE TABLE IF NOT EXISTS public."Population"
(
    id_population serial NOT NULL,
    id_demography integer NOT NULL,
    pop_ages character varying,
    gender character varying,
    "number" double precision,
    PRIMARY KEY (id_population)
);

CREATE TABLE IF NOT EXISTS public."Life Expectancy"
(
    id_life_expectancy serial NOT NULL,
    id_demography integer NOT NULL,
    gender character varying,
    value double precision,
    PRIMARY KEY (id_life_expectancy)
);

CREATE TABLE IF NOT EXISTS public."Health"
(
    id_health serial NOT NULL,
    country_code character varying NOT NULL,
    year integer,
    hospital_beds double precision,
    physicians double precision,
    expenditure double precision,
    PRIMARY KEY (id_health)
);

ALTER TABLE IF EXISTS public."Country"
    ADD FOREIGN KEY (region_code)
    REFERENCES public."Region" (region_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Survey"
    ADD FOREIGN KEY (country_code)
    REFERENCES public."Country" (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Decile"
    ADD FOREIGN KEY (id_survey)
    REFERENCES public."Survey" (id_survey) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Demography"
    ADD FOREIGN KEY (country_code)
    REFERENCES public."Country" (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Employment"
    ADD FOREIGN KEY (country_code)
    REFERENCES public."Country" (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Education"
    ADD FOREIGN KEY (country_code)
    REFERENCES public."Country" (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Economy"
    ADD FOREIGN KEY (country_code)
    REFERENCES public."Country" (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Population"
    ADD FOREIGN KEY (id_demography)
    REFERENCES public."Demography" (id_demography) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Life Expectancy"
    ADD FOREIGN KEY (id_demography)
    REFERENCES public."Demography" (id_demography) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS public."Health"
    ADD FOREIGN KEY (country_code)
    REFERENCES public."Country" (country_code) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;
    """)

    conn.commit()
    print("Tabela recriada do zero!")

except Exception as e:
    conn.rollback()
    print(f"Erro: {e}")    

# Lê o CSV e separa os dados
df_poverty = pd.read_csv('processing/poverty_inequality/Poverty_Inequality_filtered.csv')
df_region_indicators = pd.read_csv('processing/global_indicators/Global_Indicators_regions_filtered.csv')
df_indicators = pd.read_csv('processing/global_indicators/Global_Indicators_filtered.csv')

# Divide o DataFrame conforme alguma condição ou seleção de colunas
df_region = df_region_indicators[['region_name', 'region_code']]
df_country = df_poverty[['region_code', 'country_name', 'country_code']]  # Para a tabela 'Country'
df_survey = df_poverty[['country_code', 'welfare_type', 'survey_acronym', 'survey_comparability', 
                'comparable_spell', 'poverty_line', 'headcount', 'poverty_gap', 
                'poverty_severity', 'gini', 'reporting_pop', 'reporting_pce', 
                'distribution_type', 'spl', 'survey_year']]  # Para a tabela 'Survey'


# Passo 1: Carregar dados na tabela 'Region'
# Verificar duplicidade e só inserir valores únicos
df_region.drop_duplicates(subset=['region_code'], inplace=True)  # Remover duplicatas por 'region_code'

with open('Global_Indicators_regions_filtered.csv', 'w') as f:
    df_region.to_csv(f, index=False)

with open('Global_Indicators_regions_filtered.csv', 'r') as f: # Pode dar erro com duplicatas
    cursor.copy_expert("""
        COPY public."Region" (region_code, region_name)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
    """, f)

# Passo 2: Carregar dados na tabela 'Country'
# Verificar duplicidade e só inserir valores únicos
df_country.drop_duplicates(subset=['country_code'], inplace=True)  # Remover duplicatas por 'country_code'

with open('Poverty_Inequality_filtered_country.csv', 'w') as f1:
    df_country.to_csv(f1, index=False)

with open('Poverty_Inequality_filtered_country.csv', 'r') as f1: # Pode dar erro com duplicatas
    cursor.copy_expert("""
        COPY public."Country" (region_code, country_name, country_code)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
    """, f1)

# Passo 3: Carregar dados na tabela 'Survey'
with open('Poverty_Inequality_filtered_survey.csv', 'w') as f2:
    df_survey.to_csv(f2, index=False)

with open('Poverty_Inequality_filtered_survey.csv', 'r') as f2:
    cursor.copy_expert("""
        COPY public."Survey" (country_code, welfare_type, survey_acronym, survey_comparability, 
                               comparable_spell, poverty_line, headcount, poverty_gap, poverty_severity, 
                               gini, reporting_pop, reporting_pce, distribution_type, spl, survey_year)
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',')
    """, f2)

# Commit das inserções
conn.commit()

# Fechar a conexão
cursor.close()
conn.close()