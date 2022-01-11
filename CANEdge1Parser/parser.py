import datetime
import os
import re
import logging
import matplotlib.pyplot as plt

# Extracts days, hours, mins, secs, msecs from timestamp of format
# dddd HH:MM:SS.kkk
# timestamp_groups_rgx = re.compile(r"(\d+)\s(\d+)\:(\d+)\:(\d+)\.(\d+)(\s.*)")
#
datetime_format = "%Y-%m-%d %H:%M:%S.%f"

nodes_of_interest = [
    4,      # Turn motor
    5,      # Left wheel motor
    6,      # Right wheel motor
    14,     # Top camera
    15,     # Bottom camera
    16,     # Front proximity sensor
    17,     # Rear proximity sensor
    59,     # PSU
    63      # Robomoto
]

heartbeat_code = 0x700
heartbeat_ids = [heartbeat_code + node_id for node_id in nodes_of_interest] + [0x80]

psu_node_id = 59
psu_sdo_transmit_id = 0x580 + psu_node_id
psu_sdo_receive_id = 0x600 + psu_node_id
psu_sdo_ids = [psu_sdo_transmit_id, psu_sdo_receive_id]

cob_ids = heartbeat_ids + psu_sdo_ids

timestamp_rgx = re.compile(r'(\d+\.\d+)(.*)')

logger = logging.getLogger()


class CANEdge1Parser:
    def __init__(self, bot=None, input_dir=None, output_dir=None, delete_input_files=False):
        self.bot = bot
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.delete_input_files = delete_input_files
        self.processed_counter = 0
        self.skipped_counter = 0
        self.columns_processed = False
        self.active_sdo_cob_id = None
        self.active_sdo_index = None
        self.active_sdo_datetime = None

    def __check_line_for_delay(self, cob_id, line, curr_datetime, time_diff_dict, delay):
        # Check difference in timestamps
        if not time_diff_dict[cob_id]['prev_datetime']:
            time_diff_dict[cob_id]['prev_datetime'] = curr_datetime
        else:
            time_diff_dict[cob_id]['time_diff'] = curr_datetime - time_diff_dict[cob_id]['prev_datetime']
            time_diff_dict[cob_id]['prev_datetime'] = curr_datetime

            # Add time diff to line if it's defined
            line += f"\t({time_diff_dict[cob_id]['time_diff']})"

            # Check for failures
            if time_diff_dict[cob_id]['time_diff'] > datetime.timedelta(milliseconds=delay):
                time_diff_dict[cob_id]['time_diff_failures'] += 1
                line += " - FAILURE"
                if not time_diff_dict[cob_id]['time_diff_min_failure'] or time_diff_dict[cob_id]['time_diff'] < \
                        time_diff_dict[cob_id]['time_diff_min_failure']:
                    time_diff_dict[cob_id]['time_diff_min_failure'] = time_diff_dict[cob_id]['time_diff']

        line += '\n'

        return line

    def __check_line_for_sdo_delay(self, cob_id, line, curr_datetime, index, delay):
        # Check if we have start or end of transaction
        if cob_id == psu_sdo_receive_id:
            # If we have two receive ids in a row, we fail
            if self.active_sdo_cob_id == psu_sdo_receive_id:
                line += " - FAILURE - 2 receives in a row"
        elif cob_id == psu_sdo_transmit_id:
            # If we have two transmit ids in a row, we fail
            if self.active_sdo_cob_id == psu_sdo_transmit_id:
                line += " - FAILURE - 2 transmits in a row"
            else:
                # Check if indexes match
                if index == self.active_sdo_index:
                    time_diff = curr_datetime - self.active_sdo_datetime

                    # Add time diff to line
                    line += f"\t({time_diff})"

                    # Check for failures
                    if time_diff > datetime.timedelta(milliseconds=delay):
                        line += " - FAILURE"
                else:
                    line += " - FAILURE - Indexes don't match"

        self.active_sdo_cob_id = cob_id
        self.active_sdo_datetime = curr_datetime
        self.active_sdo_index = index

        line += '\n'

        return line

    def __convert_line_unixtime_to_timestamp(self, line):
        # Quick format check
        timestamp_groups = timestamp_rgx.search(line)
        if not timestamp_groups:
            return False, None
        else:
            unixtime = timestamp_groups.group(1)
            rest_of_line = timestamp_groups.group(2)
            curr_datetime = datetime.datetime.utcfromtimestamp(float(unixtime))
            curr_timestamp = curr_datetime.strftime(datetime_format)
            ret_line = curr_timestamp + rest_of_line
            return True, ret_line, curr_datetime

    def parse_file(self, in_file, out_file):
        """"Opens PSU log and parses lines."""
        # Define output files
        output_dir = self.output_dir / f"{self.bot}"
        output_file_heartbeats = output_dir / f"{self.bot}_CAN_heartbeats.log"
        output_file_psu_sdos = output_dir / f"{self.bot}_CAN_psu_sdos.log"
        output_file_timestamped = output_dir / f"{self.bot}_CAN_timestamped.log"

        # Reset counters
        self.processed_counter = 0
        self.skipped_counter = 0

        with open(in_file, 'r') as i:
            logger.info(f"Parsing: {in_file}")
            with open(output_file_heartbeats, 'a') as o:
                with open(output_file_psu_sdos, 'a') as pso:
                    with open(output_file_timestamped, 'a') as to:
                        time_diff_dict = dict()

                        # Populate dict with dictionaries for each cob_id
                        for cob_id in cob_ids:
                            time_diff_dict[cob_id] = {
                                "prev_datetime": False,
                                "time_diff_failures": 0,
                                "time_diff_min_failure": False,
                            }

                        file_columns_processed = False

                        for line in i:
                            # Split line in to fields
                            in_fields = line.strip().split(';')
                            out_fields = in_fields[1:]

                            # Process column names in first line
                            if not file_columns_processed:
                                # Only actually write columns if it's the first log file to encounter
                                if not self.columns_processed:
                                    columns = in_fields
                                    o.write(line)
                                file_columns_processed = True
                            else:
                                row_dict = dict(zip(columns, in_fields))

                                # 1. Timestamp line
                                result, out_line, curr_datetime = self.__convert_line_unixtime_to_timestamp(line)

                                if result:
                                    to.write(out_line + '\n')

                                    # 2. Append heartbeat information
                                    # Check for COBID of interest
                                    cob_id = int(row_dict['ID'], 16)

                                    if cob_id in heartbeat_ids:
                                        if cob_id == 0x80:
                                            delay = 15
                                        else:
                                            delay = 150

                                        heartbeat_line = self.__check_line_for_delay(
                                            cob_id, out_line, curr_datetime,
                                            time_diff_dict, delay)

                                        o.write(heartbeat_line)
                                    elif cob_id in psu_sdo_ids:
                                        delay = 100
                                        index = int(row_dict['DataBytes'], 16) & 0x00FFFFFF00000000
                                        sdo_line = self.__check_line_for_sdo_delay(
                                            cob_id, out_line, curr_datetime,
                                            index, delay)

                                        pso.write(sdo_line)
                            self.processed_counter += 1

    def parse_bot_folder(self):
        """Parses a bot folder with several psu log files."""

        bot_dir = self.input_dir / f"{self.bot}"

        if not bot_dir.exists():
            FileNotFoundError(f"Input directory for bot {self.bot} doesn't exist")

        logfiles = [d for d in bot_dir.glob("**/*.csv")]

        if not logfiles:
            logger.warning(f"Folder for bot {self.bot} contains no log files, exiting")
        else:
            # Check output dir exists
            output_dir = self.output_dir / f"{self.bot}"

            if not output_dir.exists():
                logger.info(f"Creating output dir for bot {self.bot}")
                output_dir.mkdir()

            # Define output file name
            # out_file = output_dir / f"{self.bot}_heartbeats.log"

            # # Check if previous file exists and delete if so
            # if out_file.exists():
            #     out_file.unlink()

            for file in logfiles:
                active_file = bot_dir / f"{file}"
                self.parse_file(active_file, self.bot)
                logger.info(
                    f"Finished processing {active_file}. "
                    f"Processed: {self.processed_counter}, "
                    f"skipped: {self.skipped_counter}"
                )
                if self.delete_input_files:
                    logger.info(f"Deleting {active_file}")
                    active_file.unlink()


class CANEdge1Plotter:
    def __init__(self, bot=None, input_dir=None, output_dir=None, cobid=None):
        self.bot = bot
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.cobid = cobid
        self.rgx = re.compile(r'([\d\-\.\:\s]+).*\(.*\.(.*)\)')
        self.started_processing = False
        self.finished_processing = False

    def __extract_fields(self, line, start_datetime, end_datetime):
        if self.cobid in line:
            fields = self.rgx.search(line)
            if fields:
                timestamp = datetime.datetime.strptime(fields.groups()[0], datetime_format)

                if not self.started_processing:
                    if timestamp > start_datetime:
                        self.started_processing = True
                elif timestamp > end_datetime:
                    self.finished_processing = True

                if self.started_processing:
                    return True, [timestamp, fields.groups()[1]]

        return False, None

    def plot_heart_beat(self, start_time, end_time):
        # Convert timestamps to datetime objects
        start_datetime = datetime.datetime.strptime(start_time, datetime_format)
        end_datetime = datetime.datetime.strptime(end_time, datetime_format)

        # Load log file
        bot_dir = self.input_dir / f"{self.bot}"
        logfiles = [d for d in bot_dir.glob("**/*.log")]

        if not logfiles:
            logger.warning(f"Folder for bot {self.bot} contains no log files, exiting")
        else:
            active_file = bot_dir / f"{logfiles[0]}"

            # Extract fields of interest
            timestamps_x = []
            values_y = []
            with open(active_file, 'r') as li_fh:
                for line in li_fh:
                    result, parsed_keys = self.__extract_fields(line, start_datetime, end_datetime)

                    if result:
                        timestamps_x.append(parsed_keys[0])
                        values_y.append(int(parsed_keys[1]))

                    if self.finished_processing:
                        break

            # Plot
            plt.plot(timestamps_x, values_y)
            plt.title(f'Bot {self.bot} - {self.cobid} - ({start_time} - {end_time})')
            plt.ylabel('Time difference')
            plt.xlabel('Timestamp')
            plt.show()
