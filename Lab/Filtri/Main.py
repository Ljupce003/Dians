import time
import Filter_I

start_time = time.time()

Filter_I.Call_Filter_1()

end_time = time.time()
duration = end_time - start_time
print(f"Program completed in {duration:.2f} seconds")
