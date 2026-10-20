        print(
            f"[WEBHOOK_REJECT] reason=missing_fields event={event_type!r} "
            f"symbol={data.get('symbol')!r} missing={missing}",
            flush=True,
        )
        return jsonify({"ok": False, "error": "missing_fields", "missing": missing}), 400

    try:
        signal_value = float(data["signal"])
        if signal_value == 0:
            raise ValueError("signal must be non-zero")
        signal = 1 if signal_value > 0 else -1
        event_time = int(float(data["time"]))
        raw_symbol = str(data["symbol"])
        symbol = normalize_symbol(raw_symbol)

        if event_type == "trend_state":
            ztype, high, low, zone_id = 0, 0.0, 0.0, ""
        else:
            ztype = 2 if int(data["type"]) == 2 else 1
            high = float(data["high"])
            low = float(data["low"])
            # Read zone_id from the incoming JSON; never reference an undefined variable.
            zone_id = str(data.get("zone_id", "") or "").strip()
    except (ValueError, TypeError, OverflowError):
        return reject_webhook("bad_field_types", data=data)

    if not symbol or event_time <= 0:
        return reject_webhook("invalid_values", data=data)
    if event_type != "trend_state" and high <= low:
        return reject_webhook("invalid_zone_bounds", data=data)

    event_id = f"{symbol}:{event_type}:{event_time}:{signal}:{ztype}:{high}:{low}:{zone_id}"
    event = {
        "id": event_id,
        "event": event_type,
        "symbol": symbol,
        "signal": signal,
        "type": ztype,
        "high": high,
        "low": low,
        "time": event_time,
        "zone_id": zone_id,
        "received": int(time.time()),
    }

    with LOCK:
        q = QUEUES[symbol]
        duplicate = any(item["id"] == event["id"] for item in q)

        if not duplicate:
            if event_type == "trend_state":
                # Trend is state, not a trade signal: keep only the latest queued trend snapshot.
                retained = deque(item for item in q if item.get("event") != "trend_state")
                retained.appendleft(event)
                QUEUES[symbol] = retained
                q = QUEUES[symbol]
            else:
                q.append(event)

            while len(q) > MAX_QUEUE:
                removable = next(
                    (item for item in q if item.get("event") != "trend_state"),
                    None,
                )
                if removable is not None:
                    q.remove(removable)
                else:
                    q.pop()

    print(
        f"[WEBHOOK] event={event_type} raw={raw_symbol!r} "
        f"normalized={symbol!r} signal={signal} type={ztype} "
        f"high={high} low={low} time={event_time} zone_id={zone_id!r} "
        f"queued={not duplicate}",
        flush=True,
    )

    return jsonify(
        {
            "ok": True,
            "queued": not duplicate,
            "id": event["id"],
            "event": event_type,
            "symbol": symbol,
        }
    )


@app.get("/poll")
def poll():
    if not authorized(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    raw_symbol = request.args.get("symbol", "")
    symbol = normalize_symbol(raw_symbol)
    if not symbol:
        return jsonify({"ok": False, "error": "missing_symbol"}), 400

    with LOCK:
        q = QUEUES.get(symbol)
        if not q:
            print(
                f"[POLL] raw={raw_symbol!r} normalized={symbol!r} event_found=False",
                flush=True,
            )
            return jsonify({"ok": True, "signal": 0, "event": "none"})

        # NOTE: this protocol still removes the event before EA acknowledgement.
        # Durable storage and acknowledgement/retry are not implemented here.
        event = q.popleft()
        remaining = len(q)

    print(
        f"[POLL] event={event['event']} raw={raw_symbol!r} "
        f"normalized={symbol!r} event_found=True signal={event['signal']} "
        f"remaining={remaining}",
        flush=True,
    )
    return jsonify(event)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
