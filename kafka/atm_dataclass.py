from dataclasses import dataclass, field
from datetime import datetime
from dataclasses_json import config, dataclass_json
from marshmallow import fields

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