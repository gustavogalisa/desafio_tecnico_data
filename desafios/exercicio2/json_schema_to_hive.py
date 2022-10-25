import json

_ATHENA_CLIENT = None

def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def handler():
    '''
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    '''

    f = open(r'C:\Users\Gustavo\Desktop\desafio_tecnico\desafios\exercicio2\schema.json')
    schema = json.load(f)
    create = ''

    # Loop para apendar junto à string do create os campos e tipos
    for i in schema['properties']:
                col_name = i
                col_type = schema['properties'][i]['type']
                create += f'{col_name} {col_type},\n'

    # Definição do tamanho da string -2, para retirar a última vírgula e evitar erro de sintaxe durante create
    exclude_comma = len(create)-2

    create_table = (f'CREATE EXTERNAL TABLE IF NOT EXISTS db_schema.table_name({create[:exclude_comma]}) \nSTORED AS PARQUET\nLOCATION \'s3://iti-query-results/[folder]/\'')