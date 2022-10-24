import json
import boto3

_SQS_CLIENT = None

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")

def fields_validation(event, schema):

    # Verifica required fields
    schema_fields = sorted(schema['required'])
    event_fields = sorted(event.keys())

    diff = set(event_fields)-set(schema_fields)
    mismatches = ', '.join(list(map(str,diff)))
    
    if event_fields != schema_fields:
        raise Exception(f'Event schema`s mismatching due {mismatches} fields.') 
    
    # Verifica address fields
    address_fields = sorted(schema["properties"]['address']['required'])
    address_event_fields = sorted(event['address'].keys())

    address_diff = set(address_event_fields)-set(address_fields)
    address_mismatches = ', '.join(list(map(str,address_diff)))
    
    if address_event_fields != address_fields:
        raise Exception(f'Event schema`s mismatching due {address_mismatches} fields.')


def types_validation(event, schema):

    # Verifica dt_types para os campos do schema
    dt_type_schema_properties = []
    for i in schema["properties"]:
        dt_type_schema_properties.append(schema["properties"][i]['type'][:3])

    # Verifica os dt_types para os campos do evento
    dt_type_event_properties = []    
    for i in event.values():
        dt_type_event_properties.append(i.__class__.__name__[:3])

    # Verifica dt_types para os campos de address do schema
    dt_type_schema_address_properties = []        
    for i in schema["properties"]['address']['properties']:
        dt_type_schema_address_properties.append(schema["properties"]['address']['properties'][i]['type'][:3])

    # Verifica dt_types para os campos de address do schema
    dt_type_event_address_properties = []
    for i in event['address'].values():
        dt_type_event_address_properties.append(i.__class__.__name__[:3])
    
    # Compara os data types do schema e evento para validá-los
    if dt_type_event_properties != dt_type_schema_properties or dt_type_schema_address_properties != dt_type_event_address_properties: 
        raise Exception('Event`s datatypes don`t match schema')

def handler(event):
    '''
    #  Função principal que é sensibilizada para cada evento
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função send_event_to_queue para envio do evento para a fila,
        não é necessário alterá-la
    '''

    f = open(r'C:\Users\Gustavo\Desktop\desafio_tecnico\desafios\exercicio1\schema.json')
    schema = json.load(f)

    fields_validation(event, schema)
    types_validation(event, schema)
    send_event_to_queue(event, 'valid-events-queue') # Alterei apenas o nome da fila
    