import json
import logging
import os
import signal
import sys
from functools import lru_cache

import pandas as pd
import redis
import requests
import yaml
from pymongo import MongoClient
from rich import print

CONFIG = {}
EMAILJS = {}
KITS = None
REPS = None
ALL_REPS = {}

HOUSE_ACCOUNT_STRING = "House or No Rep Found"

RDB = None  # redis db
NEW_FILES_QUEUE = "queue:new_files"

SENT = {}

L = None  # logger

CONFIG_PATH = os.path.join(r"C:\temp", "global", "config.yaml")
if not os.path.exists(CONFIG_PATH):
    CONFIG_PATH = os.path.join("/app", "config.yaml")
    if not os.path.exists(CONFIG_PATH):
        CONFIG_PATH = os.path.join(os.getcwd(), "config.yaml")
        assert os.path.exists(CONFIG_PATH), "Config file not found"


def init():
    global CONFIG, CONFIG_PATH, EMAILJS, KITS, REPS, ALL_REPS, L, RDB

    L = logging.getLogger("my_logger")
    L.setLevel(logging.DEBUG)

    handler = logging.FileHandler("//busse/home/lis_notifier.log")
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    L.addHandler(handler)

    with open(CONFIG_PATH) as f:
        CONFIG = yaml.load(f, Loader=yaml.FullLoader)

    uri = CONFIG.get("mongodb", {}).get("uri", None)
    assert uri is not None, "MongoDB URI not found in config.yaml"

    EMAILJS = CONFIG.get("emailjs", {})

    client = MongoClient(uri)
    db = client.get_database("busse_sales_reps")

    KITS = db.get_collection("kits")
    REPS = db.get_collection("reps")

    ALL_REPS = {rep["territory_name"]: rep["email"] for rep in REPS.find()}

    redis_config = CONFIG.get("redis", {})
    redis_url_with_port = redis_config.get("url", None)
    assert redis_url_with_port is not None, "Redis URL not found in config.yaml"
    redis_pass = redis_config.get("pass", None)
    assert redis_pass is not None, "Redis Password not found in config.yaml"

    redis_url, redis_port = redis_url_with_port.split(":")

    RDB = redis.Redis(host=redis_url, port=int(redis_port), password=redis_pass, db=0)


init()


# def push_to_redis_queue(data: dict) -> None:
#     RDB.rpush(REDIS_QUEUE, json.dumps(data))


def process_xls(
    year=None,
    purchase_order=None,
):
    if year is None:
        year = input("Enter year: (YYYY)\n> ").strip()
    if purchase_order is None:
        purchase_order = input("Enter purchase order: (E#####)\n> ").upper().strip()

    base_path = os.path.join(
        r"\\busse\Quality Control\Database$\Database$",
        f"{year} Database",
        f"Release Reports {year}",
    )

    fpath = os.path.join(base_path, f"{purchase_order}.xls")
    assert os.path.exists(fpath), f"File not found: {fpath}"

    columns = [
        "Lot Number",
        "Catalog Number",
        "Mfg. Quantity",
        " Quantity",
        "Disposition",
        "Warehouse Locations",
    ]

    dtype = {
        "Lot Number": "str",
        "Catalog Number": "str",
        "Mfg. Quantity": "str",
        " Quantity": "str",
        "Disposition": "str",
        "Warehouse Locations": "str",
    }

    df1 = pd.read_excel(
        fpath,
        sheet_name="10-08-03 (2)",
        header=6,
        usecols=columns,
        dtype=dtype,
    )

    df1.columns = [
        "lot",
        "part",
        "mfg_qty",
        "qty",
        "status",
        "note",
    ]

    df1 = df1[df1["part"].notna()]

    df1.fillna("", inplace=True)

    columns = [
        "Unnamed: 2",
        "Unnamed: 7",
    ]
    dtype = {
        "Unnamed: 2": "str",
        "Unnamed: 7": "datetime64[ns]",
    }

    df2 = pd.read_excel(
        fpath,
        sheet_name="10-08-03 (2)",
        parse_dates=True,
        header=3,
        usecols=columns,
        dtype=dtype,
    )

    df2.columns = [
        "po",
        "date",
    ]

    df2["date"] = df2["date"].dt.strftime("%B %d, %Y")

    df2 = df2[df2["date"].notna()]

    return [
        lot for lot in df1.to_dict("records") if lot["lot"] != "Comments:"
    ], df2.to_dict("records")[0]


def send_email_through_emailjs(lot):
    params = {
        "service_id": EMAILJS["service_id"],
        "template_id": EMAILJS["template_id"],
        "accessToken": EMAILJS["accessToken"],
        "user_id": EMAILJS["user_id"],
        "template_params": lot,
    }

    L.info(
        f"{lot['lot']}|{lot['part']}|{lot['status']}|{lot['note']}|{lot['po']}|{lot['date']}|{lot['mfg_qty']}|{lot['qty']}|{lot['sales_rep']}|{lot['sales_rep_email']}"
    )

    requests.post(
        "https://api.emailjs.com/api/v1.0/email/send",
        json=params,
    )


@lru_cache(maxsize=128)
def find_rep_by_kit(kit: str) -> int:
    if "R" in kit:
        kit = kit.split("R")[0]

    kit_in_db = KITS.find_one({"alias": kit})

    if kit_in_db is None:
        return HOUSE_ACCOUNT_STRING

    return kit_in_db["rep"]


def main(
    year: str = None, po: str = None, debug: bool = False, dont_send: bool = False
):
    global ALL_REPS, HOUSE_ACCOUNT_STRING

    lots, details = process_xls(year=year, purchase_order=po)

    print(lots)
    print(details)

    emails = []
    review = []

    for lot in lots:
        lot["lot"] = lot["lot"].strip()
        lot["po"] = details["po"]
        lot["date"] = details["date"]

        rep = find_rep_by_kit(lot["part"])

        lot["sales_rep"] = rep
        lot["sales_rep_email"] = ALL_REPS.get(rep, "it@busseinc.com")

        if debug:
            lot["sales_rep_email"] = "jmodell@busseinc.com,jeff@notmodells.com"

        if rep == HOUSE_ACCOUNT_STRING:
            review.append(lot)
            continue

        emails.append(lot)

    if not dont_send:
        for email in emails + review:
            send_email_through_emailjs(email)

        print("Emails sent!")

    if len(review) > 0:
        print("Review:")
        print([x["part"] for x in review])

    print()

    if len(emails) > 0:
        print("Emails:")
        print(emails)


def signal_handler(signum, frame):
    print("Exiting...")
    sys.exit(0)


def listen_to_queue(queue_name: str = NEW_FILES_QUEUE):
    global RDB

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        try:
            data = RDB.blpop(queue_name, timeout=0)

            if data is None:
                continue

            data = data[1]
            data = json.loads(data)

            if "year" in data and "file_name" in data:
                po = data["file_name"]
                if data["file_name"].lower().endswith(".xls"):
                    po = data["file_name"].split(".")[0]

                if po in SENT:
                    print(f"Skipping (Duplicate) {po}...{data['year']}")
                    continue

                SENT[po] = True

                main(
                    year=data["year"],
                    po=po,
                    debug=False,
                    dont_send=False,
                )

                print(f"Processing {po}...{data['year']}")

        except SystemExit:
            break
        except Exception as e:
            L.error(e)


if __name__ == "__main__":
    # main(
    #     debug=input("Debug? (y/n)\n> ").lower().startswith("y"),
    #     dont_send=input("Don't send? (y/n)\n> ").lower().startswith("y"),
    # )
    listen_to_queue()
