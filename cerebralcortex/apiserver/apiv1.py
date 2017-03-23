from flask import Blueprint
from flask_restplus import Api

from cerebralcortex.apiserver.apis.v1.stream import api as v1_stream
from cerebralcortex.apiserver.apis.v1.user import api as v1_user

blueprint = Blueprint('version1', __name__, url_prefix='/api/1')
api = Api(blueprint,
          title='Cerebral Cortex',
          version='1.0',
          description='API server for Cerebral Cortex',
          contact='dev@md2k.org',
          license='BSD 2-Clause',
          license_url='https://opensource.org/licenses/BSD-2-Clause'
          )

api.add_namespace(v1_user)
api.add_namespace(v1_stream)