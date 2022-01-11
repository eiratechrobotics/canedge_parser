import argparse
import os
import pathlib
import logging.config
from CANEdge1Parser.parser import CANEdge1Parser, CANEdge1Plotter

# Load CONFIG dir
log_config_var = "CANPARSERCONFIGPATH"
if log_config_var in os.environ:
    config_dir_path = os.environ[log_config_var]
else:
    raise EnvironmentError(f"Environment variable {log_config_var} not defined")
config_dir = pathlib.Path(config_dir_path)

# Load DATA dir
log_data_var = "CANPARSERDATAPATH"
if log_data_var in os.environ:
    data_dir_path = os.environ[log_data_var]
else:
    raise EnvironmentError(f"Environment variable {log_data_var} not defined")
data_dir = pathlib.Path(data_dir_path)

# Define used paths
data_input_dir = data_dir / 'input'
data_output_dir = data_dir / 'output'
data_plot_dir = data_dir / 'plot'
config_logging_file = config_dir / 'logging.conf'

# Configure logging
logging.config.fileConfig(config_logging_file)
logger = logging.getLogger()


def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--bots', nargs='+', required=True, help='List of bots to parse logs for ("*" for all bots).')
    return parser.parse_args()


def process_bots(bot_list):
    """Will process logs for all bots in list."""

    if "*" in bot_list:
        logger.info("Found \"*\" in bot list. Processing all bots")
        bot_list = [d.name for d in data_input_dir.iterdir() if d.is_dir()]

    for bot in bot_list:
        process_bot(bot)

    # executor = concurrent.futures.ProcessPoolExecutor(4)
    # futures = [executor.submit(process_bot, bot) for bot in bot_list]
    # concurrent.futures.wait(futures)
    # return [f.result() for f in futures if f.result()]


def process_bot(bot):
    if ".gitignore" in bot:
        return

    logger.info(f"Parsing robot {bot}")
    lp = CANEdge1Parser(bot, data_input_dir, data_output_dir)
    lp.parse_bot_folder()


def main():
    """Parse all log files in the input_logs dir."""

    # Read list of bots
    args = parse_args()

    logger.info(f"Processing bots: {args.bots}")
    process_bots(args.bots)
    lp = CANEdge1Plotter(args.bots[0], data_output_dir, data_plot_dir, '5BB')
    # lp.plot_heart_beat('2021-08-12 15:50:00.0', '2021-08-13 08:10:00.0')


if __name__ == "__main__":
    main()
