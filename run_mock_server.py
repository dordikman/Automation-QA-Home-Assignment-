"""
Mock server launcher for Locust load testing.

Starts the REST API Flask application wired to a live in-memory pipeline.
An asyncio event loop runs in a background thread, continuously simulating
sensor publishing, algorithm processing, and database writing so the REST
API always has data to serve.

Usage
-----
    python run_mock_server.py

Then in a separate terminal:
    locust -f tests/load/test_load.py --host=http://localhost:5000
"""

import asyncio
import threading

from mocks.algorithm_a import AlgorithmA
from mocks.algorithm_b import AlgorithmB
from mocks.data_writer import DataWriter
from mocks.rabbitmq import InMemoryBroker
from mocks.rest_api import create_app
from mocks.sensor import Sensor

_SENSOR_INTERVAL = 0.1    # seconds between audio messages (10 msg/s)
_WRITER_INTERVAL  = 0.5   # seconds between DataWriter flush cycles


async def _run_sensor(sensor: Sensor, stop: threading.Event) -> None:
    while not stop.is_set():
        await sensor.publish_audio()
        await asyncio.sleep(_SENSOR_INTERVAL)


async def _run_algo(algo, stop: threading.Event) -> None:
    """Generic loop for Algorithm A or B: process one message, back-off if idle."""
    while not stop.is_set():
        result = await algo.process_one()
        if result is None:
            await asyncio.sleep(0.01)


async def _run_writer(writer: DataWriter, stop: threading.Event) -> None:
    while not stop.is_set():
        await writer.flush()
        await asyncio.sleep(_WRITER_INTERVAL)


async def _pipeline_main(sensor, algo_a, algo_b, writer, stop: threading.Event) -> None:
    await asyncio.gather(
        _run_sensor(sensor, stop),
        _run_algo(algo_a, stop),
        _run_algo(algo_b, stop),
        _run_writer(writer, stop),
    )


def build_pipeline():
    """
    Wire up the full in-memory pipeline.

    Subscription order matters: fanout subscribers (algo_b, data_writer,
    REST API) must register before algo_a begins publishing.
    """
    broker   = InMemoryBroker()
    algo_b   = AlgorithmB(broker)          # subscribes to features_a
    writer   = DataWriter(broker)          # subscribes to features_a + features_b
    app      = create_app(broker, writer.db)  # subscribes to features_a + features_b
    algo_a   = AlgorithmA(broker)
    sensor   = Sensor(broker, sensor_id="load-test-sensor")
    return broker, sensor, algo_a, algo_b, writer, app


def main() -> None:
    broker, sensor, algo_a, algo_b, writer, app = build_pipeline()

    stop = threading.Event()

    def run_pipeline():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_pipeline_main(sensor, algo_a, algo_b, writer, stop))

    t = threading.Thread(target=run_pipeline, daemon=True, name="pipeline")
    t.start()

    print("Mock pipeline running.")
    print("Flask server starting at http://localhost:5000")
    print("Run Locust with:")
    print("  locust -f tests/load/test_load.py --host=http://localhost:5000")
    print("Press Ctrl+C to stop.\n")

    try:
        app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print("\nShutting down mock server...")
        stop.set()


if __name__ == "__main__":
    main()
