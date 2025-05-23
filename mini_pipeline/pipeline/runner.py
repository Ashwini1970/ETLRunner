# Runner module
import json
import pandas as pd
from pipeline import transform,extract,load
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def run_pipeline(config_path):
    # Load config
    with open(config_path, "r") as f:
        config = json.load(f)

    input_file = config["input_file"]
    steps = config["steps"]
    output_file = config["output_file"]

    logging.info(f"Reading input file: {input_file}")
    df = extract.read_csv(input_file)

    # Apply each step
    for step in steps:
        step_type = step["type"]
        logging.info(f"Applying step: {step}")
        if step_type == "filter":
            df = transform.filter_rows(df, step)
        elif step_type == "add_column":
            df = transform.add_column(df, step)
        elif step_type == "drop_column":
            df = transform.drop_column(df, step)
        elif step_type == "map_column":
            df = transform.map_column(df, step)
        else:
            logging.warning(f"Unknown step type: {step_type}")

    logging.info(f"Writing output to: {output_file}")
    load.write_csv(df, output_file)
    logging.info("Pipeline completed successfully.")
