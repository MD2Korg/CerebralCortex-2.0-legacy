from flask import request
from flask_restplus import Namespace, Resource, fields as rest_fields

from cerebralcortex.apiserver import CC
from cerebralcortex.kernel.datatypes.datapoint import DataPoint
from cerebralcortex.kernel.datatypes.datastream import Stream

api = Namespace('stream', description='Data and annotation streams')

data_descriptor = api.model('DataDescriptor', {
    'type': rest_fields.String(required=True),
    'unit': rest_fields.String(required=True),
    'descriptive_statistic': rest_fields.String(required=False)
})
parameter = api.model('Parameter', {
    'name': rest_fields.String(required=True),
    'value': rest_fields.Arbitrary(required=True)
})
stream_entry = api.model('Stream Entry', {
    'name': rest_fields.String(required=True),
    'identifier': rest_fields.String(required=True)
    # "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
})
execution_context = api.model('Execution Context', {
    'input_parameters': rest_fields.List(rest_fields.Nested(parameter)),
    'input_streams': rest_fields.List(rest_fields.Nested(stream_entry))
})
annotations = api.model('Annotation', {
    'name': rest_fields.String(required=True),
    'identifier': rest_fields.String(required=True)
    # "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
})
stream = api.model('Stream', {
    'identifier': rest_fields.String(required=True),
    # "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
    'owner': rest_fields.String(required=True),
    # "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
    'name': rest_fields.String(required=True),
    'data_descriptor': rest_fields.List(rest_fields.Nested(data_descriptor), required=True),
    'execution_context': rest_fields.Nested(execution_context, required=True),
    'annotations': rest_fields.List(rest_fields.Nested(annotations))
})

data_element = api.model('Data Element', {
    'start_time': rest_fields.DateTime(required=True),
    'end_time': rest_fields.DateTime(required=False),
    'sample': rest_fields.List(rest_fields.Raw(required=True))
})

stream_data = api.model('Stream Data', {
    'identifier': rest_fields.String(required=True),
    # "pattern": "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"
    'data': rest_fields.List(rest_fields.Nested(data_element), required=True)
})

# class AnnotationItem(Schema):
#     name = fields.String(required=True)
#     identifier = fields.UUID(required=True)
#
#
# class StreamEntry(Schema):
#     name = fields.String()
#     identifier = fields.UUID(required=True)
#
#
# class Parameter(Schema):
#     name = fields.String(required=True)
#     value = fields.Raw(required=True)
#
#
# class DataDescriptorItem(Schema):
#     type = fields.String(required=True)
#     unit = fields.String(required=True)
#     descriptive_statistic = fields.String()
#
#
# class ExecutionContext(Schema):
#     identifier = fields.UUID(required=True)
#     input_parameters = fields.List(fields.Nested(Parameter))
#     input_streams = fields.List(fields.Nested(StreamEntry))
#
#
# class StreamSchema(Schema):
#     identifier = fields.UUID(required=True)
#     owner = fields.UUID(required=True)
#     name = fields.String(required=True)
#     data_descriptor = fields.List(fields.Nested(DataDescriptorItem), required=True)
#     execution_context = fields.Dict(fields.Nested(ExecutionContext), required=True)
#     annotations = fields.List(fields.Nested(AnnotationItem))
#
#     @post_load
#     def make_stream(self, data):
#         return CC_Stream(**data)


STREAMS = [
    {
        "identifier": "31a84a5d-549b-480b-8b6d-9faa898894f0",
        "owner": "a970e186-e960-11e6-bf0e-fe55135034f3",
        "name": "ecg",
        "description": "RAW ecg from AutoSense",
        "data_descriptor": [
            {
                "type": "number",
                "unit": "none"
            }
        ],
        "execution_context": {

        },
        "annotations": [
            {
                "name": "study",
                "identifier": "5b7fb6f3-7bf6-4031-881c-a25faf112dd9"
            },
            {
                "name": "privacy",
                "identifier": "01dd3847-4bae-418b-8fcd-03efc4607df0"
            },
            {
                "name": "access control",
                "identifier": "d1108a2c-fe86-4adc-8d95-f8bcf379955b"
            },
            {
                "name": "platform",
                "identifier": "aec29183-3a45-4ab4-9beb-72475b3cf38a"
            },
            {
                "name": "informed consent",
                "identifier": "aec29183-3a45-4ab4-9beb-72475b3cf38b"
            }
        ]
    },
    {
        "identifier": "8405dc31-fca9-4390-840e-5c888c3dbba0",
        "owner": "a970e186-e960-11e6-bf0e-fe55135034f3",
        "name": "80th_percentile_rr_variance",
        "description": "80th percentile",
        "data_descriptor": [
            {
                "type": "number",
                "unit": "milliseconds",
                "descriptive_statistic": "80th_percentile"
            }
        ],
        "execution_context": {
            "input_parameters": [
                {
                    "name": "window_size",
                    "value": 60.0
                },
                {
                    "name": "window_offset",
                    "value": 60.0
                }
            ],
            "input_streams": [
                {
                    "name": "ecg_rr_interval",
                    "identifier": "5b7fb6f3-7bf6-4031-881c-a25faf112dd1"
                }
            ]
        },
        "annotations": [
            {
                "name": "study",
                "identifier": "5b7fb6f3-7bf6-4031-881c-a25faf112dd9"
            },
            {
                "name": "privacy",
                "identifier": "01dd3847-4bae-418b-8fcd-03efc4607df0"
            },
            {
                "name": "access control",
                "identifier": "d1108a2c-fe86-4adc-8d95-f8bcf379955b"
            },
            {
                "name": "data_source",
                "identifier": "d7cfab9d-c5c1-436f-a145-b03a7e3e1704"
            },
            {
                "name": "platform",
                "identifier": "aec29183-3a45-4ab4-9beb-72475b3cf38a"
            }
        ]
    }

]


# stream_schema = StreamSchema()


@api.route('/')
class APIStreamList(Resource):
    @api.doc('list_streams')
    @api.marshal_list_with(stream)
    def get(self):
        return STREAMS


@api.route('/<uuid:identifier>')
@api.param('identifier', 'Stream Details')
@api.response(403, 'Stream parameter error')
@api.response(404, 'Stream not found')
class APIStream(Resource):
    @api.doc('get_stream_by_identifier')
    @api.marshal_with(stream)
    def get(self, identifier):
        for u in STREAMS:
            if u['identifier'] == str(identifier):
                return u
        api.abort(404)

    @api.doc('create_stream_by_identifier')
    @api.marshal_with(stream)
    def post(self, identifier):
        parameters = request.json

        if str(identifier) != parameters['identifier']:
            return api.abort(403, "URL identifier mismatch with JSON document request")

        try:

            new_stream = Stream(identifier=parameters['identifier'],
                                owner=parameters['owner'],
                                name=parameters['name'],
                                description=parameters['description'],
                                data_descriptor=parameters['data_descriptor'],
                                execution_context=parameters['execution_context'],
                                annotations=parameters['annotations'])

            result = CC.update_or_create(new_stream)
            CC.save_stream(result)  # TODO: Should the semantics be here or automatically within CC
            return parameters, 201

        except KeyError as e:
            return api.abort(404, "Named parameter not found: " + str(e))





@api.route('/<uuid:identifier>/data')
@api.param('identifier', 'stream identifier')
@api.response(403, 'Stream parameter error')
@api.response(404, 'Stream not found')
class APIStreamData(Resource):
    @api.doc('put data into stream')
    @api.marshal_with(stream_data)
    def put(self, identifier):
        parameters = request.json

        if str(identifier) not in ['5b7fb6f3-7bf6-4031-881c-a25faf112dd9', '5b7fb6f3-7bf6-4031-881c-a25faf112ddf']:
            return api.abort(404, "Identifier not found")

        if str(identifier) != parameters['identifier']:
            return api.abort(403, "URL identifier mismatch with JSON document request")

        ds = CC.get_stream(identifier)

        try:
            for di in parameters['data']:  # TODO: Test this loop's performance
                if 'end_time' in di:
                    ds.data.append(DataPoint(start_time=di['start_time'], end_time=di['end_time'], sample=di['sample']))
                else:
                    ds.data.append(DataPoint(start_time=di['start_time'], sample=di['sample']))

        except KeyError as e:
            return api.abort(404, "Named parameter not found: " + str(e))

        return parameters, 201
