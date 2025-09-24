import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileProcessorHandler(FileSystemEventHandler):

    def __init__(self, processor_function, redis_instance):
        self.processor_function = processor_function
        self.redis_instance = redis_instance

    def on_created(self, event): # Called when a file or directory is created.
        if not event.is_directory and event.src_path.endswith('.xml'):
            print(f"Detected new file: {event.src_path}")

            thread = threading.Thread(      # This is crucial for handling bursts of files from multiple machines.
                target=self.processor_function,
                args=(event.src_path, self.redis_instance)
            )
            thread.start()

def create_observer(watch_directory, processor_function, redis_instance):

    event_handler = FileProcessorHandler(processor_function, redis_instance)
    observer = Observer()
    observer.schedule(event_handler, watch_directory, recursive=False)

    return observer
