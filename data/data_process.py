#This file is designed so that it generates processed data from raw, with the order given in "python_files_running_order.txt". 

import subprocess

subprocess.call(["python DC02_identify_relevant_NAICS.py"], cwd="./processing/DC02/", shell=True)
subprocess.call(["python DC01_collection.py"], cwd="./processing/DC01/", shell=True)
subprocess.call(["python PD01_create_weighted_index.py"], cwd="./processing/PD01/", shell=True)
subprocess.call(["python PD02_proportional_change_for_stock.py"], cwd="./processing/PD02/", shell=True)
subprocess.call(["python PD03_collection.py"], cwd="./processing/PD03/", shell=True)
subprocess.call(["python DCO02_collecting_ppi.py"], cwd="./collecting/", shell=True)
subprocess.call(["python pd04_monthly-to-daily-pro-ppi\ copy.py"], cwd="./processing/PD04/", shell=True)
subprocess.call(["python PD05_collection.py"], cwd="./processing/PD05/", shell=True)
