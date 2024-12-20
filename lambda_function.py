import json
from unittest.mock import inplace
from datetime import date, timedelta
from marketdata_collector.eom_collector import eom_sse, eom_szse, eom_bse
from marketdata_collector.volume_collector import volume_sse, volume_szse
from marketdata_collector.comm_tools import retriever

def lambda_handler(event, context):
    # TODO implement
    eom_sse.execute()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
