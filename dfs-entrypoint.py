import argparse

parser = argparse.ArgumentParser(description="mysql-dataflowsync by Jinx@qqAys in Oct 2024")
parser.add_argument(
    "--mode", "-m",
    type=str,
    choices=["cdc", "dpu", "monitor"],
    required=True,
    help="Selecting the mysql-dataflowsync startup method"
)
args = parser.parse_args()
mode = str(args.mode).lower()

if mode == "cdc":
    from cdc.binlog_processor import CDC
    cdc = CDC()
    cdc.start()
elif mode == "dpu":
    from dpu.queue_processor import DPU
    dpu = DPU()
    dpu.start()
elif mode == "monitor":
    from uvicorn import run
    from fastapi_monitor import app
    run(app, host="0.0.0.0", port=3000, log_level="info")
else:
    pass
