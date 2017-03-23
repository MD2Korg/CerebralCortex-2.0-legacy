import os

from cerebralcortex.CerebralCortex import CerebralCortex

configuration_file = os.path.join(os.path.dirname(__file__), 'cerebralcortex.yml')

CC = CerebralCortex(configuration_file, master="local[*]", name="Memphis cStress Development App")
