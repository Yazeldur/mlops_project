from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from datetime import date, datetime, timedelta
import numpy as np
from marshmallow import fields
import operator

@dataclass_json
@dataclass
class Data:
    date: datetime = field(
        metadata=config(
            encoder = datetime.isoformat,
            decoder = datetime.fromisoformat,
            mm_field = fields.DateTime(format='iso')
        )
    )
    id: int
    argent_retire: int

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def atm_script():
    '''
    Script to randomly generate a number of transactions and the retrieval amount every hour, for 150h, for 50 ATMs.
    Retuns: list of date and time, ATM ID, and retrieval amount
    '''
    atms_data = []
    starting_datetime = datetime(2022, 11, 30, 00, 00, 00)
    min = 0

    #50 ATM machines
    for id in range(50):
        transaction_time = starting_datetime
        #300 hours of transactions
        for hours in range(300):
            #Random number of transactions per hour
            h = 60
            while h > 0:    
                Argent_retire = np.random.randint(2, 200)*10
                min = np.random.randint(5,50)
                h -= min
                transaction_time += timedelta(minutes=min)
                data = Data(transaction_time, id, Argent_retire)
                atms_data.append(data)
    atms_data = sorted(atms_data, key=operator.attrgetter("date"))
    return atms_data

def to_json(atms_data):
    '''
    Creates json from list
    '''
    json_atms_data = Data.schema().dumps(atms_data, many=True)
    with open("data.json", "w+") as f:
        f.write(str(json_atms_data))

atms_data = atm_script()
to_json(atms_data)