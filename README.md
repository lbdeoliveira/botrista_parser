# botrista_parser

Lucas De Oliveira

## Overview

This repo contains a Python script that parses machine logs to identify the percentage of "dispense incomplete" events out of all orders placed and report the number of dispense incomplete events that resolved themselves after the customer placed another order (referred to here as "self resolved" events).

This script was written to be run from the terminal as follows:

`python log_parser.py log_dir output_file`

where `log_dir` is the directory name/path for where the logs are stored and `output_file` is the name/path of a CSV file that stores the results for each log.

The script does the following:

* Prints the total number of orders placed across all log files in the log directory
* Prints the percentage of those orders that are incomplete (service is stopped for some reason)
* Prints the number of "dispense warnings" that are resolved after user orders again
* Saves a CSV file `output_file` that contains the file name, number of orders, number of incomplete orders, and number of self-resolved orders in each log