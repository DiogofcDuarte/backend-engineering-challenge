import json
import argparse
import time
from collections import deque
from datetime import datetime, timedelta

def process_json_file(file_path, window_size):
    queue = deque()
    current_timestamp = None
    sum_delivery_time = 0

    # Open the file with the json
    with open(file_path, 'r') as file:
        # Read and process each line as necessary instead of reading the file in its entirity storing it and proccessing it after
        # Should help with bigger files
        for line in file:
            data = json.loads(line.strip())
            event_timestamp = datetime.strptime(data['timestamp'], '%Y-%m-%d %H:%M:%S.%f')
        
            # Grab the timestamp from the first event as the baseline
            if current_timestamp is None:
                current_timestamp = event_timestamp.replace(second=0, microsecond=0)
                
            # While the current event timestamp hasn't been reached we won't add it to the queue
            # So we keep increasing the fake timer to fake the passage of time,
            # Every minute we print the average delivery time of all    
            # Here we also remove all the older events that no longer fit the time window 
            while event_timestamp >= current_timestamp:
                average_delivery_time = round(sum_delivery_time / len(queue), 1) if queue else 0
                output = {"date": current_timestamp.strftime('%Y-%m-%d %H:%M:%S'), "average_delivery_time": average_delivery_time}
                print(json.dumps(output))
                current_timestamp += timedelta(minutes=1)
                while queue and queue[0][0] < current_timestamp - timedelta(minutes=window_size):
                    removed_event = queue.popleft()
                    sum_delivery_time -= removed_event[1]
                    
            # Here it means the current event is contained in the time window so we add it to the queue and increase the delivery time sum
            queue.append((event_timestamp, data['duration']))
            sum_delivery_time += data['duration']

    # Print the last sliding window
    average_delivery_time = round(sum_delivery_time / len(queue), 1)
    output = {"date": current_timestamp.strftime('%Y-%m-%d %H:%M:%S'), "average_delivery_time": average_delivery_time}
    print(json.dumps(output))
    queue.clear()

def main():
    # Uncomment to track the time in seconds that the app takes to run
    # start_time = time.time()
    parser = argparse.ArgumentParser(description='Process JSON file with sliding window.')
    parser.add_argument('--input_file', type=str, required=True, help='Path to the input JSON file')
    parser.add_argument('--window_size', type=int, required=True, help='Size of the sliding window in minutes')

    args = parser.parse_args()
    input_file = args.input_file
    window_size = args.window_size

    process_json_file(input_file, window_size)

    # Uncomment to track the time in seconds that the app takes to run
    # end_time = time.time()
    # execution_time = round(end_time - start_time,4)
    # print("Execution time:", execution_time, "seconds")

if __name__ == "__main__":
    main()
