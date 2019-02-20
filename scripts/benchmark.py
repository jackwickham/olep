#!/bin/python3
import os
import argparse
import tempfile
import subprocess
import time
import random
import signal

if not os.path.isfile("./gradlew"):
    if os.path.isfile("../gradlew"):
        os.chdir("../")
    else:
        print("This script should be run in the same directory as gradlew")
        exit(1)

parser = argparse.ArgumentParser(description="Run a benchmark on the olep database")
parser.add_argument("config_file", help="The yml file that is the base config file for the benchmark run")
parser.add_argument("-c", "--config", nargs="+", action="append",
    help="A config option that should be changed between benchmark runs, and the values it should be given. Multiple " \
    + "options can be specified, and the lattice of them will be tested",
    required=True, metavar=("SETTING", "VALUE"))
parser.add_argument("-t", "--time", default=610, help="Number of seconds to run the benchmark for", type=int)
args = parser.parse_args()

# Load the original config file into memory
with open(args.config_file) as f:
    base_config_file = f.read()

def all_combinations(base_options, set_options, remaining_options):
    if len(remaining_options) == 0:
        run(base_options, set_options)
    else:
        option_name = remaining_options[0][0]
        values = remaining_options[0][1:]
        # For each value, generate the combinations of the remaining args
        for v in values:
            set_options[option_name] = v
            all_combinations(base_options, set_options, remaining_options[1:])

def run(base_options, set_options):
    global args

    # Create the new config file
    with tempfile.NamedTemporaryFile(mode="w", prefix="config-", suffix=".yml", newline='\n') as new_config_file:
        new_config_file.write(base_config_file)
        for key, value in set_options.items():
            new_config_file.write(f"\n{key}: {value}")
        new_config_file.flush()

        print(f"Running with options {set_options}")

        # Run reset synchronously
        subprocess.run(["./gradlew", "reset", f'--args=--all {new_config_file.name}'], check=True)

        # Choose a file to use for IPC, so the database can report when it's ready
        characters = "abcdefghijklmnopqrstuvwxyz0123456789_"
        ready_file = tempfile.gettempdir() + "/ready-" + "".join(random.choices(characters, k=8))

        # Start the runDatabase task asynchronously
        database_process = subprocess.Popen(["./gradlew", "runDatabase", f'--args=--ready-file {ready_file} {new_config_file.name}'])

        # Wait for it to either terminate or write to ready_file
        while database_process.poll() is None and not os.path.isfile(ready_file):
            time.sleep(1)

        # Hopefully, it didn't terminate
        if database_process.returncode is not None:
            print("Database population failed")
            exit(1)

        print("Database populated successfully")

        # Now start the application
        app_process = subprocess.Popen(["./gradlew", "runApp", f'--args={new_config_file.name}'])

        # Wait 10s then make sure it's still running
        time.sleep(10)
        if app_process.poll() is not None:
            print("App shut down unexpectedly")
            # kill the database
            database_process.send_signal(signal.SIGINT)
            database_process.wait()
            exit(1)

        # Now wait the remaining time
        time.sleep(args.time - 10)

        # Make sure the processes are still running
        if app_process.poll() is not None:
            print("App shut down unexpectedly")
            database_process.send_signal(signal.SIGINT)
            database_process.wait()
            exit(1)
        if database_process.poll() is not None:
            print("Database shut down unexpectedly")
            app_process.send_signal(signal.SIGINT)
            app_process.wait()
            exit(1)

        print("Shutting down")

        # Send the shutdown signals, shutting down the app before the database
        app_process.send_signal(signal.SIGINT)
        app_process.wait()
        database_process.send_signal(signal.SIGINT)
        database_process.wait()

        # And we are done!


# Run it!
all_combinations(base_config_file, dict(), args.config)