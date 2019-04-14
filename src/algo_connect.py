import logging
import json
import os, sys, time
import signal
from kiteconnect import KiteConnect
from datetime import datetime
import datetime as delta
from common_utils.data_mining import data_frame, stochastic_osc
from flask import Flask, request, jsonify
import schedule
from os import getenv
from threading import Thread
from common_utils import constant as const


logging.basicConfig(level=logging.DEBUG)

kite = KiteConnect(api_key=const.API_KEY)

sys.path.append(os.path.dirname(os.path.abspath(os.path.join(__file__, '..'))))
app = Flask(__name__)


@app.route('/trading', methods=['POST'])
def extreme():
    data = json.loads(request.get_data(as_text=True))
    response = main(data)
    return jsonify(response), 200


def main(data):
    requestToken = data['trading']['request_token']
    access = {}
    try:
        access = kite.generate_session(request_token=requestToken, api_secret=const.API_SECRET)
        access_token = kite.set_access_token(access['access_token'])
    except Exception as TokenError:
        logging.info("Error {}".format(TokenError))
    return access


from_date = str(datetime.date(datetime.now() - delta.timedelta(days=3))) + " 21:00:00"
to_date = str(datetime.date(datetime.now())) + " 23:00:00"
interval = "5minute"

time_stamp_9am = str(datetime.date(datetime.now())) + " 09:15:00"
dt_obj = datetime.strptime(time_stamp_9am, '%Y-%m-%d %H:%M:%S')
start_time = dt_obj.timestamp()

time_stamp_11pm = str(datetime.date(datetime.now())) + " 22:55:00"
dt_obj = datetime.strptime(time_stamp_11pm, '%Y-%m-%d %H:%M:%S')
stop_time = dt_obj.timestamp()

""" Historical Candle is provided for current day intraday period as well,
    ou need to keep from= date as start time of today's date(2019-03-05+09:30:00) 
    and end time as today's date(2019-03-05).For minute period,it will show all 
    minute candle data formed till that time."""
null = 0
last_buy_order = [{
    "average_price": 0,
    "cancelled_quantity": 0,
    "disclosed_quantity": 0,
    "exchange": "MCX",
    "exchange_order_id": null,
    "exchange_timestamp": null,
    "exchange_update_timestamp": null,
    "filled_quantity": 0,
    "guid": "10871X9j11LeE4UuRR",
    "instrument_token": 54426119,
    "market_protection": 0,
    "order_id": "190325001179793",
    "order_timestamp": "2019-03-25 11:40:04",
    "order_type": "MARKET",
    "parent_order_id": null,
    "pending_quantity": 1,
    "placed_by": "ZQ5720",
    "price": 0,
    "product": "CO",
    "quantity": 1,
    "status": "PUT ORDER REQ RECEIVED",
    "status_message": null,
    "tag": null,
    "tradingsymbol": "CRUDEOILM19APRFUT",
    "transaction_type": "SELL",
    "trigger_price": 4107,
    "validity": "DAY",
    "variety": "co"
}]
last_sell_order = [{
    "average_price": 0,
    "cancelled_quantity": 0,
    "disclosed_quantity": 0,
    "exchange": "MCX",
    "exchange_order_id": null,
    "exchange_timestamp": null,
    "exchange_update_timestamp": null,
    "filled_quantity": 0,
    "guid": "10871X9j11LeE4UuRR",
    "instrument_token": 54426119,
    "market_protection": 0,
    "order_id": "190325001179793",
    "order_timestamp": "2019-03-25 11:40:04",
    "order_type": "MARKET",
    "parent_order_id": null,
    "pending_quantity": 1,
    "placed_by": "ZQ5720",
    "price": 0,
    "product": "CO",
    "quantity": 1,
    "status": "PUT ORDER REQ RECEIVED",
    "status_message": null,
    "tag": null,
    "tradingsymbol": "CRUDEOILM19APRFUT",
    "transaction_type": "BUY",
    "trigger_price": 4107,
    "validity": "DAY",
    "variety": "co"
}]


def get_historical_data():
    try:
        data = kite.historical_data(54426119, from_date=from_date, to_date=to_date, interval=interval)
        # data = kite.historical_data(54334983, from_date="2019-03-02", to_date="2019-03-19", interval="5minute")
        # logging.info("Data fetched successfully")
        data_manger = data_frame(data, 6, 4, 10)
        return data_manger
    except Exception as DataFetchError:
        logging.info("info {}".format(DataFetchError))


def over_strategy(data_manager):
    # ltp = last_ltp()
    Dvalue, Jvalue = stochastic_osc(data_manager)
    global last_buy_order
    # time = convertTimeToIndex(data_manager['time'])
    if data_manager['MA4'].iloc[-1] + 2 > data_manager['MA10'].iloc[-1] and (last_buy_order[0]['transaction_type'] == "SELL"):

        last_buy_order = place_order(kite.TRANSACTION_TYPE_BUY)
        logging.info("Place an Buy order {}".format(datetime.now()))

    # if moving average is lesser than last tick and there is a position
    elif 100 in list(Dvalue.Dvalue.tail(10)) and 90 > Dvalue.Dvalue.iloc[-1] > 88 or data_manager['MA4'].iloc[-1] < 2 + data_manager['MA10'].iloc[-1] \
            and (last_buy_order[0]['transaction_type'] == "BUY"):

        last_exit_order = exit_order(last_buy_order[0]['order_id'])
        last_buy_order[0]['transaction_type'] = "SELL"
        logging.info("Exit BUY order")
    else:
        logging.info("Wait your time is coming")

    return last_buy_order


def under_strategy(data_manager):
    # ltp = last_ltp()
    Dvalue, Jvalue = stochastic_osc(data_manager)
    global last_sell_order
    # time = convertTimeToIndex(data_manger['time'])
    if data_manager['MA4'].iloc[-1] < 2 + data_manager['MA10'].iloc[-1] and (last_sell_order[0]['transaction_type'] == "BUY"):

        last_sell_order = place_order(kite.TRANSACTION_TYPE_SELL)
        logging.info("Place an Sell order {}".format(datetime.now()))

    elif 0 in list(Dvalue.Dvalue.tail(10)) and 12 > Dvalue.Dvalue.iloc[-1] > 8 or data_manager['MA4'].iloc[-1] + 2 > data_manager['MA10'].iloc[-1] \
            and (last_sell_order[0]['transaction_type'] == "SELL"):
        last_exit_order = exit_order(last_sell_order[0]['order_id'])
        last_sell_order[0]['transaction_type'] = "BUY"
        logging.info("Exit SELL or place an BUY order")
    else:
        logging.info("time has came")

    return last_sell_order


def place_order(transaction_type):
    order_data = get_historical_data()
    if transaction_type == kite.TRANSACTION_TYPE_BUY:
        trigger_price = int(order_data['close'].iloc[-1]) - 30
    else:
        trigger_price = int(order_data['close'].iloc[-1]) + 30
    order_id = kite.place_order(tradingsymbol='CRUDEOILM19APRFUT', exchange='MCX', quantity=1,
                                trigger_price=trigger_price,
                                transaction_type=transaction_type,
                                variety=kite.VARIETY_CO, order_type="MARKET", product="MIS", validity=kite.VALIDITY_DAY)
    order_history = kite.order_history(order_id)
    order_json = json.dumps(order_history, indent=4, sort_keys=True, default=str)
    order_object = json.loads(order_json)
    return order_object


def exit_order(order_id):
    order_exit = kite.exit_order(variety=kite.VARIETY_CO, order_id=order_id, parent_order_id=None)
    return order_exit


def last_ltp():
    order_ltp = kite.ltp('MCX:CRUDEOILM19APRFUT')
    return order_ltp


def start():
    records = get_historical_data()
    over_strategy(records)
    under_strategy(records)


def exit_program():
    os.kill(os.getpid(), signal.SIGTERM)


def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    # logging.info (app.url_map)
    logging.info('root dir : {}'.format(os.path.dirname(os.path.abspath(os.path.join(__file__, '..')))))
    logging.info(os.path.dirname(os.path.abspath(os.path.join(__file__, '..'))))
    logging.info("PYTHON PATH : {}".format(getenv("PYTHONPATH")))
    logging.info("Environment: {}".format(getenv('python_env', 'local')))
    logging.info("SYSTEM PATH : {}".format(sys.path))

    if start_time < datetime.now().timestamp() < stop_time:
        schedule.every().minute.do(start)
    th = Thread(target=run_schedule)
    th.start()
    port = int(os.getenv("VCAP_APP_PORT") or 5000)
    app.run(host=const.IP, port=port, debug=False)
