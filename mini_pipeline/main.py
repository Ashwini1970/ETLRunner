import argparse
from pipeline import runner

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the mini data pipeline")
    parser.add_argument("--config", required=True, help="Path to the config file")
    args = parser.parse_args()
    
    runner.run_pipeline(args.config)
