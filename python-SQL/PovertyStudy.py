import psycopg2
import pandas as pd
import io # Importar io para StringIO

# --- Configuração (Melhor prática: usar variáveis de ambiente ou config) ---
DB_NAME = 'postgres'
DB_USER = 'postgres'
DB_PASS = '1001' # Considere usar métodos mais seguros
DB_HOST = 'localhost'
DB_PORT = '5432'

# --- Funções Auxiliares ---
def execute_sql_commands(cursor, commands):
    """Executa uma lista de comandos SQL."""
    for command in commands:
        try:
            print(f"Executando: {command[:100]}...") # Mostra o início do comando
            cursor.execute(command)
            print("Sucesso.")
        except psycopg2.Error as e:
            print(f"Erro ao executar comando: {command[:100]}...")
            print(f"Erro PG: {e}")
            raise # Re-levanta a exceção para ser capturada pelo bloco principal

def df_to_sql_copy(df, table_name, columns, cursor):
    """
    Usa COPY FROM STDIN para carregar um DataFrame no PostgreSQL
    usando um buffer em memória (StringIO).
    """
    buffer = io.StringIO()
    # Escreve o DF no buffer como CSV, com cabeçalho, sem índice
    df.to_csv(buffer, index=False, header=True, sep=',')
    buffer.seek(0) # Volta ao início do buffer

    copy_sql = f"""
        COPY public."{table_name}" ({", ".join(f'"{c}"' for c in columns)})
        FROM STDIN
        WITH (FORMAT csv, HEADER true, DELIMITER ',');
    """
    try:
        print(f"Carregando dados na tabela {table_name}...")
        cursor.copy_expert(sql=copy_sql, file=buffer)
        print(f"Dados carregados com sucesso em {table_name}.")
    except psycopg2.Error as e:
        print(f"Erro durante o COPY para a tabela {table_name}.")
        print(f"Erro PG: {e}")
        raise # Re-levanta a exceção

# --- Conexão com o Banco de Dados ---
conn = None # Inicializa conn como None
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = False # Desativa autocommit para controlar transações
    cursor = conn.cursor()
    print("Conexão com o banco de dados estabelecida.")

    # --- Definição do Schema (DDL) ---
    # Lista de comandos para criar tabelas (separados para melhor depuração)
    # Nota: Nomes de tabelas/colunas com espaços ou maiúsculas requerem aspas duplas
    create_table_commands = [
        """
        CREATE TABLE IF NOT EXISTS public."Region"
        (
            region_code character varying NOT NULL,
            region_name character varying NOT NULL, -- Assumindo que não pode ser nulo
            PRIMARY KEY (region_code)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public."Country"
        (
            country_code character varying NOT NULL,
            country_name character varying,
            region_code character varying NOT NULL,
            PRIMARY KEY (country_code)
            -- FK será adicionada depois
        );
        """,
        """
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
            -- FK será adicionada depois
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public."Decile"
        (
            id_decile serial NOT NULL,
            value double precision,
            name character varying,
            id_survey integer NOT NULL,
            PRIMARY KEY (id_decile)
            -- FK será adicionada depois
        );
        """,
        """
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
            -- FK será adicionada depois
        );
        """,
        """
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
            -- FK será adicionada depois
        );
        """,
        """
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
            -- FK será adicionada depois
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public."Economy"
        (
            id_economy serial NOT NULL,
            year integer,
            country_code character varying NOT NULL,
            gdp double precision,
            inflation double precision,
            tax_revenue double precision,
            PRIMARY KEY (id_economy)
            -- FK será adicionada depois
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public."Population"
        (
            id_population serial NOT NULL,
            id_demography integer NOT NULL,
            pop_ages character varying,
            gender character varying,
            "number" double precision, -- "number" é uma palavra reservada, precisa de aspas
            PRIMARY KEY (id_population)
            -- FK será adicionada depois
        );
        """,
        """
        -- Cuidado com nome de tabela com espaço! Requer aspas duplas sempre.
        CREATE TABLE IF NOT EXISTS public."Life Expectancy"
        (
            id_life_expectancy serial NOT NULL,
            id_demography integer NOT NULL,
            gender character varying,
            value double precision,
            PRIMARY KEY (id_life_expectancy)
             -- FK será adicionada depois
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public."Health"
        (
            id_health serial NOT NULL,
            country_code character varying NOT NULL,
            year integer,
            hospital_beds double precision,
            physicians double precision,
            expenditure double precision,
            PRIMARY KEY (id_health)
            -- FK será adicionada depois
        );
        """
    ]

    # Lista de comandos para adicionar chaves estrangeiras
    # NOT VALID pode ser removido se você quiser validação imediata (pode ser mais lento)
    add_foreign_key_commands = [
        """
        ALTER TABLE IF EXISTS public."Country"
            ADD CONSTRAINT fk_country_region FOREIGN KEY (region_code)
            REFERENCES public."Region" (region_code) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Survey"
            ADD CONSTRAINT fk_survey_country FOREIGN KEY (country_code)
            REFERENCES public."Country" (country_code) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Decile"
            ADD CONSTRAINT fk_decile_survey FOREIGN KEY (id_survey)
            REFERENCES public."Survey" (id_survey) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Demography"
            ADD CONSTRAINT fk_demography_country FOREIGN KEY (country_code)
            REFERENCES public."Country" (country_code) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Employment"
            ADD CONSTRAINT fk_employment_country FOREIGN KEY (country_code)
            REFERENCES public."Country" (country_code) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Education"
            ADD CONSTRAINT fk_education_country FOREIGN KEY (country_code)
            REFERENCES public."Country" (country_code) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Economy"
            ADD CONSTRAINT fk_economy_country FOREIGN KEY (country_code)
            REFERENCES public."Country" (country_code) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Population"
            ADD CONSTRAINT fk_population_demography FOREIGN KEY (id_demography)
            REFERENCES public."Demography" (id_demography) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Life Expectancy"
            ADD CONSTRAINT fk_lifeexp_demography FOREIGN KEY (id_demography)
            REFERENCES public."Demography" (id_demography) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """,
        """
        ALTER TABLE IF EXISTS public."Health"
            ADD CONSTRAINT fk_health_country FOREIGN KEY (country_code)
            REFERENCES public."Country" (country_code) MATCH SIMPLE
            ON UPDATE NO ACTION ON DELETE NO ACTION NOT VALID;
        """
    ]

    # --- Executar Criação de Tabelas ---
    print("--- Iniciando criação/verificação de tabelas ---")
    execute_sql_commands(cursor, create_table_commands)
    print("--- Tabelas criadas/verificadas com sucesso ---")

    # --- Executar Adição de Chaves Estrangeiras ---
    # É melhor adicionar FKs depois que todas as tabelas existem
    print("--- Iniciando adição/verificação de chaves estrangeiras ---")
    execute_sql_commands(cursor, add_foreign_key_commands)
    print("--- Chaves estrangeiras adicionadas/verificadas com sucesso ---")

    # Commit das alterações de schema
    conn.commit()
    print("Alterações no schema (DDL) commitadas.")

    # --- Leitura e Preparação dos Dados ---
    print("Lendo arquivos CSV...")
    try:
        # Ajuste os caminhos conforme necessário
        df_poverty = pd.read_csv('processing/poverty_inequality/Poverty_Inequality_filtered.csv')
        df_region_indicators = pd.read_csv('processing/global_indicators/Global_Indicators_regions_filtered.csv')
        # df_indicators = pd.read_csv('processing/global_indicators/Global_Indicators_filtered.csv') # Não usado no código original para carregar dados
    except FileNotFoundError as e:
        print(f"Erro: Arquivo CSV não encontrado - {e}")
        raise # Para o script se o arquivo não for encontrado

    print("Preparando DataFrames...")
    # Seleção CORRETA de colunas e remoção de duplicatas onde apropriado

    # Region: Selecionar colunas e garantir unicidade de region_code
    df_region = df_region_indicators[['region_code', 'region_name']].copy() # Usar .copy() para evitar SettingWithCopyWarning
    df_region.drop_duplicates(subset=['region_code'], inplace=True)
    df_region.dropna(subset=['region_code', 'region_name'], inplace=True) # Garante que PK e outras colunas NOT NULL não sejam nulas
    region_cols = ['region_code', 'region_name']

    # Country: Selecionar colunas e garantir unicidade de country_code
    df_country = df_poverty[['country_code', 'country_name', 'region_code']].copy()
    df_country.drop_duplicates(subset=['country_code'], inplace=True)
    df_country.dropna(subset=['country_code', 'region_code'], inplace=True) # Garante que PK e FK não sejam nulas
    country_cols = ['country_code', 'country_name', 'region_code'] # Ordem como será no COPY

    # Survey: Selecionar colunas. REMOVIDO drop_duplicates por country_code.
    # Adicione filtragem/lógica aqui se precisar de apenas uma survey por país.
    survey_cols = [
        'country_code', 'welfare_type', 'survey_acronym', 'survey_comparability',
        'comparable_spell', 'poverty_line', 'headcount', 'poverty_gap',
        'poverty_severity', 'gini', 'reporting_pop', 'reporting_pce',
        'distribution_type', 'spl', 'survey_year'
    ]
    df_survey = df_poverty[survey_cols].copy()
    df_survey.dropna(subset=['country_code'], inplace=True) # FK não pode ser nula

    # --- Carregamento dos Dados (DML) ---
    # Ordem importa por causa das Foreign Keys: Region -> Country -> Survey

    print("--- Iniciando carregamento de dados ---")

    # Passo 1: Carregar 'Region'
    if not df_region.empty:
        df_to_sql_copy(df_region, "Region", region_cols, cursor)
    else:
        print("DataFrame 'Region' está vazio. Nenhum dado para carregar.")

    # Passo 2: Carregar 'Country'
    if not df_country.empty:
        # Ordem das colunas no df_country deve corresponder a country_cols
        # Reordenar se necessário: df_country = df_country[country_cols]
        df_to_sql_copy(df_country, "Country", country_cols, cursor)
    else:
        print("DataFrame 'Country' está vazio. Nenhum dado para carregar.")

    # Passo 3: Carregar 'Survey'
    if not df_survey.empty:
         # Ordem das colunas no df_survey deve corresponder a survey_cols
         # Reordenar se necessário: df_survey = df_survey[survey_cols]
        df_to_sql_copy(df_survey, "Survey", survey_cols, cursor)
    else:
        print("DataFrame 'Survey' está vazio. Nenhum dado para carregar.")


    # (Adicione aqui o carregamento das outras tabelas se tiver os dataframes)


    # Commit final da transação de dados
    conn.commit()
    print("--- Carregamento de dados concluído e commitado ---")

except (Exception, psycopg2.Error) as error:
    print("\n--- Ocorreu um erro! ---")
    print(f"Erro: {error}")
    if conn:
        print("Realizando rollback...")
        conn.rollback() # Desfaz a transação atual em caso de erro
        print("Rollback concluído.")

finally:
    # Fechar cursor e conexão
    if conn:
        if cursor:
            cursor.close()
        conn.close()
        print("Conexão com o banco de dados fechada.")