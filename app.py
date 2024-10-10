from flask import Flask
from ropositories.save_csv_to_mongo import init_car_accidents

app = Flask(__name__)


@app.route('/')
def init_db():
    init_car_accidents()
    return 'welcomev to car accidends analystics!'


if __name__ == '__main__':
    app.run()
