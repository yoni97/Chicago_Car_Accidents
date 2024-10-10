from flask import Flask
from routes.accident_route import accidents_bp
from routes.injury_route import injury_bp

app = Flask(__name__)

app.register_blueprint(accidents_bp)
app.register_blueprint(injury_bp)




if __name__ == '__main__':
    init_db()
    app.run()
