from urllib.error import HTTPError
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, PrimaryKeyConstraint, Float
from sqlalchemy.orm import sessionmaker

### Variáveis para configuracao da carga de dados
url_csv = 'https://raw.githubusercontent.com/gsgrouplabs/datasets/main/base_de_respostas_10k_amostra.csv'
url_conexao_bd = 'postgresql+psycopg2://user_gs:udig78ujghs@54.94.241.13/gsc'
### Variáveis para configuracao da carga de dados

# Declarative_base é usado para mapear o banco de dados para objetos, permitindo o acesso via classes
Base = declarative_base()
class Pais(Base):
    __tablename__ = 'pais'
    __table_args__ = {'extend_existing': True}

    index = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(50))

    def __repr__(self):
        return self.nome


class Sistema_operacional(Base):
    __tablename__ = 'sistema_operacional'
    __table_args__ = {'extend_existing': True}

    index = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(50))

    def __repr__(self):
        return self.nome


class Empresa(Base):
    __tablename__ = 'empresa'
    __table_args__ = {'extend_existing': True}

    index = Column(Integer, primary_key=True, autoincrement=True)
    tamanho = Column(String(100))

    def __repr__(self):
        return self.tamanho


class Linguagem_programacao(Base):
    __tablename__ = 'linguagem_programacao'
    __table_args__ = {'extend_existing': True}

    index = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(20))

    def __repr__(self):
        return self.nome


class Ferramenta_comunicacao(Base):
    __tablename__ = 'ferramenta_comunicacao'
    __table_args__ = {'extend_existing': True}

    index = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(20))

    def __repr__(self):
        return self.nome


class RespondenteLinguagem_programacao(Base):
    __tablename__ = 'respondentelinguagem_programacao'
    # __table_args__ = {'extend_existing': True}

    respondente_index = Column(Integer, ForeignKey('respondente.index', ondelete='CASCADE'))
    linguagem_programacao_index = Column(Integer, ForeignKey('linguagem_programacao.index', ondelete='CASCADE'))
    momento = Column(Integer)

    __table_args__ = (
        PrimaryKeyConstraint('respondente_index', 'linguagem_programacao_index'),
        {'extend_existing': True},
    )


class RespondenteFerramenta_comunicacao(Base):
    __tablename__ = 'respondenteferramenta_comunicacao'
    # __table_args__ = {'extend_existing': True}

    respondente_index = Column(Integer, ForeignKey('respondente.index', ondelete='CASCADE'))
    ferramenta_comunicacao_index = Column(Integer, ForeignKey('ferramenta_comunicacao.index', ondelete='CASCADE'))
    momento = Column(Integer)

    __table_args__ = (
        PrimaryKeyConstraint('respondente_index', 'ferramenta_comunicacao_index'),
        {'extend_existing': True},
    )


class Respondente(Base):
    __tablename__ = 'respondente'
    __table_args__ = {'extend_existing': True}

    index = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(100))
    contrib_open_source = Column(String(3))
    programa_hobby = Column(Integer)
    salario = Column(Float)
    sistema_operacional_index = Column(Integer, ForeignKey('sistema_operacional.index', ondelete='CASCADE'))
    pais_index = Column(Integer, ForeignKey('pais.index'))
    empresa_index = Column(Integer, ForeignKey('empresa.index'))

    def __repr__(self):
        return self.nome


# Método que retorna uma lista de valores entre ";" presentes na coluna passada como parâmetro
def extrair_lista_multivalues(df, coluna):
    '''
    Este método retorna uma lista com todas as opções diferentes presentes no dataframe na coluna, sem duplicidades
    É útil para gerar novas tabelas com os dados.

    Parâmetros:
    df = dataframe contendo todos os dados
    coluna = nome da coluna que se deseja extrar a lista

    Exemplo de uso:
        comunication_tools = extrair_lista_multivalues(df=dados, coluna='CommunicationTools')
    '''
    df_aux = df[coluna]
    lista = []
    for index, row in df_aux.iteritems():
        for element in pd.Series(row).fillna(0).tolist():
            lista.append(element)

    # Eliminando linguagens repetidas
    lista = list(dict.fromkeys(lista))

    return lista


# Método que retorna uma lista de valores entre ";" presentes na coluna passada como parâmetro
def extrair_lista_singlevalues(df, coluna):
    '''
    Este método retorna uma lista com todas as opções diferentes presentes no dataframe na coluna especificada,
    sem duplicidades
    É útil para gerar novas tabelas com os dados.

    Parâmetros:
    df = dataframe contendo todos os dados
    coluna = nome da coluna que se deseja extrar a lista

    Exemplo de uso:
        paises = extrair_lista_singlevalues(df=dados, coluna='Country')
    '''
    df_aux = df[coluna]
    lista = []
    for index, row in df_aux.iteritems():
        # for element in pd.Series(row).fillna(0).tolist():
        lista.append(row)

    # Eliminando linguagens repetidas
    lista = list(dict.fromkeys(lista))

    return lista

try:
    ### Etapa 1 -  Carregando os dados
    dados = pd.read_csv(url_csv)
    print('Etapa 1/4 - Leitura de dados finalizada com sucesso...')

    ### Etapa 2 -  Transformação inicial dos dados (processos de limpeza e transformação)
    # Filtrando somente as colunas a serem trabalhadas no desafio
    dados = dados.loc[:, ('Respondent', 'OpenSource', 'Hobby', 'ConvertedSalary', 'CommunicationTools',
                          'LanguageWorkedWith', 'OperatingSystem', 'Country', 'CompanySize')]

    # Transformando as colunas multivalues em colunas com List
    dados['LanguageWorkedWith'] = dados['LanguageWorkedWith'].str.split(';')
    dados['CommunicationTools'] = dados['CommunicationTools'].str.split(';')

    # Preenchendo as colunas Nan com 0 ou '-', conforme necessidade
    dados.loc[:, 'ConvertedSalary'].fillna(0, inplace=True)
    dados.loc[:, 'OperatingSystem'].fillna('-', inplace=True)
    dados.loc[:, 'CompanySize'].fillna('-', inplace=True)
    dados.loc[:, 'Country'].fillna('-', inplace=True)
    dados.loc[:, 'LanguageWorkedWith'].fillna('-', inplace=True)
    dados.loc[:, 'CommunicationTools'].fillna('-', inplace=True)

    # Convertendo salário para R$ e incluindo em uma nova coluna, considerando valor de conversão como $1 = R$5.60
    dados['salario_em_BRL'] = dados.loc[:, 'ConvertedSalary'] * 5.60
    dados['salario_em_BRL'] = dados['salario_em_BRL'].astype('float')

    # Transformando Respondent, agregando coluna 'Respondent' com respectivo index
    rows = len(dados.index)
    dados['Respondent'] = dados.Respondent.astype(str) + '_' + dados.index.astype(str)
    print('Etapa 2/4 - Processos de limpeza e transformação finalizada com sucesso... ')


    ### Etapa 3 - Separação de dados multivalues e de relacionamentos para criação de tabelas de dados
    # Obtendo as listas multivalues
    comunication_tools = extrair_lista_multivalues(df=dados, coluna='CommunicationTools')
    linguagens = extrair_lista_multivalues(df=dados, coluna='LanguageWorkedWith')

    # Obtendo as listas singlevalues
    paises = extrair_lista_singlevalues(df=dados, coluna='Country')
    sistemas_operacionais = extrair_lista_singlevalues(df=dados, coluna='OperatingSystem')
    tamanhos_empresas = extrair_lista_singlevalues(df=dados, coluna='CompanySize')
    print('Etapa 3/4 - Separação de dados multivalues e de relacionamento para criação de respectivas tabelas de dados '
          'finalizada com sucesso...')

    ### Etapa 4 - Iniciando carga de dados para o banco de dados
    print('Etapa 4 - Separação de dados multivalues e de relacionamento para criação de respectivas tabelas de dados '
          'iniciando...')

    # Engine para ativar o sqlalchemy e fazer as comunicações com BD
    engine = create_engine(url_conexao_bd, echo=False)
    Session = sessionmaker(bind=engine)

    # Usando o mapeamento para deletar as tabelas para criação e execução da carga de dados FULL
    RespondenteFerramenta_comunicacao.__table__.drop(engine, checkfirst=True)
    RespondenteLinguagem_programacao.__table__.drop(engine, checkfirst=True)
    Respondente.__table__.drop(engine, checkfirst=True)
    Ferramenta_comunicacao.__table__.drop(engine, checkfirst=True)
    Linguagem_programacao.__table__.drop(engine, checkfirst=True)
    Empresa.__table__.drop(engine, checkfirst=True)
    Sistema_operacional.__table__.drop(engine, checkfirst=True)
    Pais.__table__.drop(engine, checkfirst=True)
    print('Etapa 4.1 - Tabelas pré existentes finalizada com sucesso...')

    # Criando tabelas e populando-as no BD

    # Nestes passos, as listas são transformadas em DataFrame para depois salvar no BD com df.to_sql

    # Criando tabelas Communication Tools
    df_comunications = pd.DataFrame({'nome': comunication_tools})
    df_comunications.to_sql('ferramenta_comunicacao', engine.connect(), if_exists='replace')
    with engine.connect() as conn:
        conn.execute('ALTER TABLE ferramenta_comunicacao ADD PRIMARY KEY (index);')
    print('Etapa 4.2 - Tabela e dados Communication_tools finalizada com sucesso...')

    # Criando tabelas Linguagens de Programação
    df_linguagens = pd.DataFrame({'nome': linguagens})
    df_linguagens.to_sql('linguagem_programacao', engine.connect(), if_exists='replace')
    with engine.connect() as conn:
        conn.execute('ALTER TABLE linguagem_programacao ADD PRIMARY KEY (index);')
    print('Etapa 4.3 - Tabela e dados Linguagens_programacao finalizada com sucesso...')

    # Criando tabelas Países
    df_paises = pd.DataFrame({'nome': paises})
    df_paises.to_sql('pais', engine.connect(), if_exists='append')
    with engine.connect() as conn:
        conn.execute('ALTER TABLE pais ADD PRIMARY KEY (index);')
    print('Etapa 4.4 - Tabela e dados Países finalizada com sucesso...')

    # Criando tabelas Sistemas Operacionais
    df_sos = pd.DataFrame({'nome': sistemas_operacionais})
    df_sos.to_sql('sistema_operacional', engine.connect(), if_exists='replace')
    with engine.connect() as conn:
        conn.execute('ALTER TABLE sistema_operacional ADD PRIMARY KEY (index);')
    print('Etapa 4.5 - Tabela e dados Sistemas_operacionais finalizada com sucesso...')

    # Criando tabelas Empresas
    df_empresas = pd.DataFrame({'tamanho': tamanhos_empresas})
    df_empresas.to_sql('empresa', engine.connect(), if_exists='replace')
    with engine.connect() as conn:
        conn.execute('ALTER TABLE empresa ADD PRIMARY KEY (index);')
    print('Etapa 4.6 - Tabela e dados Empresas finalizada com sucesso...')

    # Inserindo a tabela Respondent e seus relacionamentos com sistema_operacional, pais e empresa
    # Selecionando apenas as colunas necessárias
    df_respondent = dados.loc[:, ('Respondent', 'OpenSource', 'Hobby', 'salario_em_BRL', 'OperatingSystem', 'Country',
                                  'CompanySize')]

    # Recuperando o index de OperatinSystem, Country e CompanySize
    for index, row in df_respondent.iterrows():
        # Buscando as chaves que foram incluídas em BD
        df_respondent.loc[index, 'OperatingSystem'] = int(
            df_sos.loc[(df_sos.nome == str(df_respondent.loc[index, 'OperatingSystem']))].index[0]
        )
        df_respondent.loc[index, 'Country'] = int(
            df_paises.loc[(df_paises.nome == df_respondent.loc[index, 'Country'])].index[0]
        )
        df_respondent.loc[index, 'CompanySize'] = int(
            df_empresas.loc[(df_empresas.tamanho == df_respondent.loc[index, 'CompanySize'])].index[0]
        )

    df_respondent = df_respondent.rename(columns={'Respondent': 'nome', 'OpenSource': 'contrib_open_source',
                                                  'Hobby': 'programa_hobby', 'salario_em_BRL': 'salario',
                                                  'OperatingSystem': 'sistema_operacional_index',
                                                  'Country': 'pais_index', 'CompanySize': 'empresa_index'})

    df_respondent.to_sql('respondente', engine.connect(), if_exists='replace')
    with engine.connect() as conn:
        conn.execute(
            'ALTER TABLE respondente ADD PRIMARY KEY (index);'
            'ALTER TABLE respondente ADD FOREIGN KEY (sistema_operacional_index) REFERENCES sistema_operacional(index);'
            'ALTER TABLE respondente ADD FOREIGN KEY (pais_index) REFERENCES pais(index); '
            'ALTER TABLE respondente ADD FOREIGN KEY (empresa_index) REFERENCES empresa(index); '
        )
    print('Etapa 4.7 - Tabela e dados Respondentes finalizada com sucesso...')

    # Inserindo as tabelas RespondenteLinguagem_programacao e RespondenteLinguagem_programacao

    # Selecionando apenas as colunas necessárias
    df_aux = dados.loc[:, ('CommunicationTools', 'LanguageWorkedWith')]

    # Criando os dataframes para armazenamento dos dados
    df_RespondenteFerramenta_comunicacao = pd.DataFrame(columns={'respondente_index', 'ferramenta_comunicacao_index'})
    df_RespondenteLinguagem_programacao = pd.DataFrame(columns={'respondente_index', 'linguagem_programacao_index'})

    # Percorrendo o Dataframe para buscar as opções escolhidas pelos respondentes
    # Dentro do loop, será necessário recuperar o Index de cada opção do respondente para gerar a tabela
    # de relacionamento NxN correspondente
    for index, row in df_aux.iterrows():
        # Recuperando as listas de ferramentas e linguagens
        lista_tools = df_aux.loc[index, 'CommunicationTools']
        lista_linguagens = df_aux.loc[index, 'LanguageWorkedWith']

        # Gerando a tabela de relacionamento N x N - Ferramenta de comunicacao
        for x in lista_tools:
            # Recupera o id da ferramenta e atribui para index_ferramenta, depois criar um dataframe provisório (df2),
            # e adiciona ao df_RespondenteFerramenta_comunicacao
            index_ferramenta = df_comunications.loc[df_comunications.nome == x].index.values.astype(int)[0]
            df2 = pd.DataFrame([[index_ferramenta, index]],
                               columns=['ferramenta_comunicacao_index', 'respondente_index'])
            df_RespondenteFerramenta_comunicacao = df_RespondenteFerramenta_comunicacao.append(df2)

        # Gerando a tabela de relacionamento N x N - Linguagens de programação
        for x in lista_linguagens:
            # Recupera o id da linguagem e atribui para index_linguagem, depois criar um dataframe provisório (df2),
            # e adicionar ao df_RespondenteLinguagem_programacao
            index_linguagem = df_linguagens.loc[df_linguagens.nome == x].index.values.astype(int)[0]
            df2 = pd.DataFrame([[index_linguagem, index]], columns=['linguagem_programacao_index', 'respondente_index'])
            df_RespondenteLinguagem_programacao = df_RespondenteLinguagem_programacao.append(df2)

    # Inserindo valores na tabela RespondenteFerramenta_comunicacao
    df_RespondenteFerramenta_comunicacao.to_sql(name='respondenteferramenta_comunicacao', con=engine.connect(),
                                                if_exists='replace', index=False)
    # Incluindo constraints de PK e FK na tabela criada pelo .to_sql
    with engine.connect() as conn:
        conn.execute(
            'ALTER TABLE "respondenteferramenta_comunicacao" '
            '   ADD PRIMARY KEY (respondente_index,ferramenta_comunicacao_index);'
            'ALTER TABLE "respondenteferramenta_comunicacao" '
            '   ADD FOREIGN KEY (respondente_index) REFERENCES Respondente(index);'
            'ALTER TABLE "respondenteferramenta_comunicacao" '
            '   ADD FOREIGN KEY (ferramenta_comunicacao_index) REFERENCES Ferramenta_comunicacao(index);'
        )
    print('Etapa 4.8 - Tabela e dados do relacionamento NxN entre Respondente e Ferramenta_comunicados '
          'finalizada com sucesso...')

    # Inserindo valores na tabela RespondenteFerramenta_comunicacao
    df_RespondenteLinguagem_programacao.to_sql(name='respondentelinguagem_programacao', con=engine.connect(),
                                               if_exists='replace', index=False)
    # Incluindo constraints de PK e FK na tabela criada pelo .to_sql
    with engine.connect() as conn:
        conn.execute(
            'ALTER TABLE "respondentelinguagem_programacao" '
            '   ADD PRIMARY KEY (respondente_index,linguagem_programacao_index);'
            'ALTER TABLE "respondentelinguagem_programacao" '
            '   ADD FOREIGN KEY (respondente_index) REFERENCES Respondente(index);'
            'ALTER TABLE "respondentelinguagem_programacao" '
            '   ADD FOREIGN KEY (linguagem_programacao_index) REFERENCES Linguagem_programacao(index);'
        )
    print('Etapa 4.9 - Tabela e dados do relacionamento NxN entre Respondente e Linguagem_programacao '
          'criados com sucesso...')

    print('Etapa 4/4 - Carga de dados para o banco de dados realizada com finalizada com sucesso...')
    print('Fim da carga de dados.')

except HTTPError as e:
    print("Oops!  Não foi possível encontrar o arquivo especificado. "
          "Verifique a URL e tente iniciar a carga novamente...")
    print('Detalhe do erro: ' + str(e))

except OperationalError as e:
    print('Oops! Não foi possível efetuar operação com o banco de dados.')
    print('Detalhe do erro: ' + str(e))

except Exception as e:
    print('Oops! Ocorreu algum problema durante a carga de dados.')
    print('Detalhe do erro: ' + str(e))
