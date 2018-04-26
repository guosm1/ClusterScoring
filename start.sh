#!/usr/bin/env bash
python -u score.py >> log/score.log 2>&1 &
# sleep 5
# python -u train_lstm.py >> ../log/lstm.log 2>&1 &
# python -u hadoop.py >> ../log/collectors.log 2>&1 &
