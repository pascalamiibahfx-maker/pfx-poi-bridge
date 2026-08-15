from flask import Flask, request, jsonify
from threading import Lock
from collections import defaultdict, deque
import os, json, time

app = Flask(__name__)
LOCK = Lock()
QUEUES = defaultdict(deque)
TOKEN = os.environ.get('PFX_BRIDGE_TOKEN', 'CHANGE_ME_LONG_RANDOM_TOKEN')
MAX_QUEUE = int(os.environ.get('PFX_MAX_QUEUE', '100'))


def authorized(req):
    supplied = req.headers.get('X-PFX-TOKEN', '') or req.args.get('token', '')
    return supplied == TOKEN


@app.get('/health')
def health():
    return jsonify({'ok': True})


@app.post('/webhook')
def webhook():
    if not authorized(request):
        return jsonify({'ok': False, 'error': 'unauthorized'}), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({'ok': False, 'error': 'invalid_json'}), 400

    required = ['symbol', 'signal', 'type', 'high', 'low', 'time']
    if any(k not in data for k in required):
        return jsonify({'ok': False, 'error': 'missing_fields'}), 400

    try:
        signal = 1 if float(data['signal']) > 0 else -1
        ztype = 2 if int(data['type']) == 2 else 1
        high = float(data['high'])
        low = float(data['low'])
        event_time = int(float(data['time']))
        symbol = str(data['symbol'])
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'error': 'bad_field_types'}), 400

    if not symbol or high <= low or event_time <= 0:
        return jsonify({'ok': False, 'error': 'invalid_values'}), 400

    event = {
        'id': f'{symbol}:{event_time}:{signal}:{ztype}:{high}:{low}',
        'symbol': symbol,
        'signal': signal,
        'type': ztype,
        'high': high,
        'low': low,
        'time': event_time,
        'received': int(time.time())
    }

    with LOCK:
        q = QUEUES[symbol]
        # Do not enqueue exact duplicates.
        if not any(x['id'] == event['id'] for x in q):
            q.append(event)
            while len(q) > MAX_QUEUE:
                q.popleft()

    return jsonify({'ok': True, 'id': event['id']})


@app.get('/poll')
def poll():
    if not authorized(request):
        return jsonify({'ok': False, 'error': 'unauthorized'}), 401

    symbol = request.args.get('symbol', '')
    if not symbol:
        return jsonify({'ok': False, 'error': 'missing_symbol'}), 400

    with LOCK:
        q = QUEUES.get(symbol)
        if not q:
            return jsonify({'ok': True, 'signal': 0})
        event = q.popleft()

    return jsonify(event)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', '8080'))
    app.run(host='0.0.0.0', port=port)
