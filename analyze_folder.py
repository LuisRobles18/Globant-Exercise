from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import time
import subprocess

class EventHandler(FileSystemEventHandler):
    #Only if the folder is not empty we execute the script that reads the CSV files
    def on_created(self, event):
        subprocess.run(["python", "read_csv_files.py"])


#We create an observer which allow us to check for any file changes from a specific folder
if __name__ == "__main__":
    path = "./data/incoming"
    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()