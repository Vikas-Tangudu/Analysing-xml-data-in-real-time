import os
import time
import configparser
from redis_setup import initialize_redis
from observer_setup import create_observer
from xml_processor import process_xml_file

# Read configuration from the file
config = configparser.ConfigParser()
config.read('config.ini')

WATCH_DIRECTORY = config['app']['watch_directory']
REDIS_HOST = config['redis']['host']
REDIS_PORT = int(config['redis']['port'])

def main():

    if not os.path.exists(WATCH_DIRECTORY):
        os.makedirs(WATCH_DIRECTORY)
        print(f"Created watch directory: {WATCH_DIRECTORY}")

    redis_instance = initialize_redis(REDIS_HOST, REDIS_PORT) # Since redis lib is thread safe, using single client connection to avoid connection overhead.

    observer = create_observer(WATCH_DIRECTORY, process_xml_file, redis_instance)
    observer.start()
    print(f"Started file watcher on directory: {WATCH_DIRECTORY}")

    # Keep the main thread alive to allow the observer to run in the background
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("Watcher stopped.")
    finally:
        observer.join() # To make sure all the background threads terminated succesfully.

if __name__ == "__main__":
    main()
