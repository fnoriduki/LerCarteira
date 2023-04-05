import boto3
import json
import decimal

from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)



print('Loading function')
dynamo = boto3.client('dynamodb')
client = boto3.client('cognito-identity')


from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
ts= TypeSerializer()
td = TypeDeserializer()

def dynamo_obj_to_python_obj(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v) 
        for k, v in dynamo_obj.items()
    }  

def from_dynamodb_to_json(item):
    d = TypeDeserializer()
    return {k: d.deserialize(value=v) for k, v in item.items()}
    
def python_obj_to_dynamo_obj(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }    

def lambda_handler(event, context):
    

    
    response = client.get_id(
        IdentityPoolId='us-east-1:...',
        Logins={
            'cognito-idp.us-east-1.amazonaws.com/us-east-1_pG2pWfJY3': event['params']['header']['Authorization']
            
        }
    )

    carteira = dynamo.get_item(
        TableName='tbCarteira',
        Key={
            'clientid': {'S': response['IdentityId'] }
        },
        ProjectionExpression = "tickers"
    )
    
     #----------------------------- Filtro  ----------------------#
    
    carteira = dynamo_obj_to_python_obj(carteira['Item'])['tickers']
    filtro = []
    for item in carteira:
        item_filtro = dict()
        item_filtro['ticker'] = item['ticker']
        filtro.append(item_filtro)
    print('filtro:')
    print(filtro)
    #----------------------------- Filtro  ----------------------#
    
    #----------------------------- captura precos  ----------------------#
    print('evento:')
    print(((carteira)))
    print('<Fim do evento>')
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table("tbPreco")

    # Define the partition key and sort keys for the two items we want to get.
    precos = []
    try:
        response = dynamodb.batch_get_item(
            RequestItems={
                'tbPreco':{
                    'Keys':filtro,
                    'ConsistentRead': False # For my user case, this data it is not changed often so why not get the reads at half price? Your use case might be different and need True.
                }
            },
            ReturnConsumedCapacity='TOTAL'
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        precos = response['Responses']['tbPreco']
        
   #     precos= json.dumps(precos, cls=DecimalEncoder)
        print("BatchGetItem succeeded:")
        print(json.dumps(precos, indent=4, cls=DecimalEncoder))
    #----------------------------- captura precos  ----------------------#
    #----------------------------- JOIN  ----------------------#    
    dicionario_precos = dict()
    for row in precos:
        dicionario_precos[row["ticker"]] = row
    print(dicionario_precos)
    for row in carteira:

      if (row['ticker'] in dicionario_precos):
        row['valor_mercado']= Decimal(dicionario_precos[row['ticker']]['valor'])
        row['database']= dicionario_precos[row['ticker']]['database'] 
        row['total_pago'] = Decimal(row['qtd'])*Decimal(row['preco_medio'])
        row['total_mercado'] = Decimal(row['qtd'])*Decimal(row['valor_mercado'])
        row['total_resultado'] = Decimal(row['total_mercado']) - Decimal(row['total_pago'])
    
 
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin':'*'
        },
        'body': carteira
       
    }
    
