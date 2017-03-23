from apiv1 import blueprint as apiv1
from flask import Flask

app = Flask(__name__)
app.debug = True
app.secret_key = 'cc_development'

app.register_blueprint(apiv1)


if __name__ == "__main__":
    app.run()
