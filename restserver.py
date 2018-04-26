from flask import Flask

import conf
import resource_manager
import score

app = Flask(__name__)

cfg = conf.Config("./conf/config.json")
rmq = resource_manager.RMQueue()

@app.route('/')
def api_root():
  return 'Welcome, this is hadoop yarn scoring and predicting system.'


@app.route('/update/app')
def update_app():
  score.update_app_info(rmq, cfg)
  return 'You are updating job info'


@app.route('/update/cluster')
def update_cluster():
  score.update_cluster_info(rmq, cfg)
  return 'You are updating cluster info'


@app.route('/update/scheduler')
def update_scheduler():
  score.update_scheduler_info(rmq, cfg)
  return 'You are updating scheduler info'


@app.route('/update/all')
def update_all():
  score.update_all_info(rmq, cfg)
  return 'You are updating job, cluster and scheduler info'


@app.route('/config', methods=['POST'])
def config():
  return 'You are updating config file information of all pathes'


@app.route('/score')
def get_score():
  score.score(rmq, cfg)
  return 'You are asking to score the hadoop yarn system'


@app.route('/predict')
def predict():
  score.predict(rmq, cfg)
  return 'You are asking the prediction of hadoop yarn system'


@app.route('/ping')
def ping():
  return ('ok')


def start_server(port=5000):
  rmq.display()
  cfg.display()
  app.run(host='127.0.0.1', port=port)


if __name__ == '__main__':
  app.run(host='127.0.0.1', port=5001)
