#!/usr/bin/env python
# coding: utf-8
import argparse
import sys
import os
import json
import pandas as pd
import ntpath
import time

parser = argparse.ArgumentParser(description='Description of your app.')
parser.add_argument('inputDirectory',help='Path to the input directory.')
parser.add_argument('-u',action='store_true' ,dest='timestamp' ,default=False , help='change Timestamp')

parsed_args = parser.parse_args()

if os.path.exists(parsed_args.inputDirectory):
	
	
	for filename in os.listdir(parsed_args.inputDirectory):
		#print("File exist")
		start_time = time.time()
		records = [json.loads(line) for line in open(parsed_args.inputDirectory+'/'+filename)]
		df_records = pd.DataFrame(records)
		df_rec = pd.DataFrame()
		df_rec['web_browser'] = df_records['a'].str.split().str[0].str.strip() 
		df_rec['operating_sys'] = df_records['a'].str.extract(r"\((.*?)\)", expand=False) 
		df_rec['from_url'] = df_records['r'] 
		df_rec['to_url'] = df_records['u'] 
		df_rec['city'] = df_records['cy'] 
		df_rec['longitude'] = df_records['ll'].str.get(0) 
		df_rec['latitude'] = df_records['ll'].str.get(1) 
		df_rec['time_zone'] = df_records['tz'] 
		if parsed_args.timestamp:
			df_rec['time_in'] = df_records['t'] 
			df_rec['time_out'] = df_records['hc'] 
		else :
			df_rec['time_in'] = pd.to_datetime(df_records['t'] ,unit='ms') 
			df_rec['time_out'] = pd.to_datetime(df_records['hc'],unit='ms') 
	
		df_rec=df_rec.dropna()
		total_rows =df_rec.shape[0]
		df_rec.to_csv(r'/home/mhassan01/ITI_Python_for_Data_Managment/Task_2/target/'+filename+'_file.csv',header=True, index = False)
		print("------Scrpit info Details for file Name : "+filename+"-----------")
		print("Total row tranformed : "+str(total_rows))
		print("Traget Data Path : /home/mhassan01/ITI_Python_for_Data_Managment/Task_2/target/")
		print("Execution time : %s seconds." % (time.time() - start_time))
		print("----------Done --Thanks-------------")	


else :
	print("readable_dir: "+ parsed_args.inputDirectory +" is not a valid path")
