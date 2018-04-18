import os
import re
import sys
import time
import hashlib
from threading import Thread
import logging


class Timer():
	'''
	   Simple loop which keeps record of the time
	'''

	def __init__(self):	
		self.t = 0
		
		
	def timer(self):
	
		while True:
			time.sleep(1)
			self.t += 1
	
	
	def main(self):
	
		tt = Thread(target=self.timer)
		tt.daemon = True
		tt.start()
	
		return 0
		
	def time_elapsed(self):
		return self.t




class InputClass():
	''' Input class for getting user input 
	Memory	size>	2000	
	Memory	management	policy	(1-	VSP,	2-	PAG,	3-	SEG)>	1	
	Fit	algorithm	(1-	first-fit,	2-	best-fit)>	2	
	If	the	policy	is	PAG,	prompt	for	a	page/frame	size	(rather	than	a	fit	algorithm).	
	'''
	
	def __init__(self):
	
		logging.debug('Get User input.')
		self.mem_size = self.memory_size_input()
		self.policy, self.param = self.memory_mgt_policy()
		
	def memory_size_input(self):
	
		mem_size = raw_input('Memory Size> ')
		
		if re.findall(r'^[0-9]+$', mem_size, re.I):
			return mem_size
		else:
			logging.error('Cannot understand the user input.')
			sys.exit()	


	def memory_mgt_policy(self):
	
		policy = raw_input('Memory management policy> \n 1. VSP \n 2. PAG \n 3. SEG\n')

		param = '0'
		
		if policy in ['1', '3']:
			param = raw_input('Memory management policy> \n 1. First Fit \n 2. Best Fit\n')
		elif policy == '2':
			pass
		else:
			logging.error('Cannot understand the user input.')
			sys.exit()
			
		policy_dict = {
		
			'1' : 'VSP',
			'2' : 'PAG',
			'3' : 'SEG'
		}
		
		param_dict = {
		
			'0' : 'None',
			'1' : 'First Fit',
			'2' : 'Best Fit'
		}
		
		return policy_dict[policy], param_dict[param]


		

class OutputClass():

	def __init__(self):
	
		logging.debug('Output class.')
		


class WorkloadClass():
	''' Workload class format
	
	Total Process:8
	Process Info:
	Process ID:
	Arrival time:   lifetime:
	Total Chunks: Chunks:
	
	
	'''

	def __init__(self):
		#logging.debug('Workload class.')
		self.parameters = ['Process ID', 'Arrival time', 'lifetime', 'Total Chunks', 'Chunks']

		
	def list_of_workload(self, workload_dir):
		return [x for x in os.listdir('workloads') if x not in workload_dir]
	
	
	def workload_chunk(self, workload_dir):
		''' Returns the workload chunk in a file'''
	
		temp = ''
		file_list = self.list_of_workload(workload_dir)
		#logging.debug('Found new files : {}'.format(file_list))
		
		for file_name in file_list:
			with open(os.path.join('workloads', file_name), 'r') as f:
				temp += f.read()
				
		return temp.split('\n\n'), file_list
			
	
	def parse_work_loads(self, workload_dir):
	
		index_chunk = 0
		workload_dict = dict()

		workload_raw, file_list = self.workload_chunk(workload_dir)

		for workload_chunk in workload_raw:
			
			
			temp = dict()
			work_list = workload_chunk.split('\n')
	
			if work_list:
			
				# Remove empty lines in the workload
				if '' in work_list:
					work_list.remove('')	
			
				if work_list and index_chunk == 0:
					work_list.pop(0)

				if work_list and len(work_list) == 3:

					temp['Process ID'] = work_list[0]
					temp['Arrival time'] = work_list[1].split(' ')[0]
					temp['lifetime'] = work_list[1].split(' ')[1]
					temp['Total Chunks'] = work_list[2].split(' ')[0]
					temp['Chunks'] = [x for y, x in enumerate(work_list[2].split(' ')) if y != 0]
					temp['hash'] = hashlib.sha224(str(work_list)).hexdigest()
					temp['st_time'] = 0
					temp['seg'] = 0		
					temp['mem_address'] = list()
					temp['proc_id'] = 0
					temp['type'] = ''

					# Do not add in the file if this is missing
					if not temp['Chunks'] or not temp['lifetime']:
						logging.error('Key pieces of information missing in workload file {}'.format(workload_chunk))
						continue
						
					workload_dict[index_chunk] = temp
					index_chunk += 1
					
				
				
				else:
					#logging.error('Issue in the work load file format {}'.format(work_list))
					pass
			else:
				pass
				#logging.error("No working list.")
				
		#logging.debug("New files converted into work load dictionaries : {}".format(workload_dict))
		return workload_dict, file_list
		
	def main(self, workload_dir):
		return self.parse_work_loads(workload_dir)


class MM():

	def __init__(self, ui):
		self.t_max = 100000
		self.mem_size = int(ui.mem_size)
		self.policy = ui.policy
		self.param = ui.param
	
	
	def gand(self):
	
		process_lifetime = dict()
		old_list = list()
		tt = Timer()
		tt.main()
		
		mem_store = self.create_mem_buffer()
		
		#l = {'Process ID': '1', 'Total Chunks': '1', 'Arrival time': '0', 'Chunks': ['4', '8', '7'], 'lifetime': '16', 'hash': '533d1e2e5573aed87fe58f77c23dc01e515ceb76677678156e67e2d9', 'type':'', 'seg': '', 'st_time': 10, 'proc_id': ''}
		#m = {'Process ID': '2', 'Total Chunks': '1', 'Arrival time': '0', 'Chunks': ['14'], 'lifetime': '1000', 'hash': '533d1e2e5573aed87fe58f77c23dc01e515ceb76677678156e67e2d9', 'type':'', 'seg': '', 'st_time': 0, 'proc_id': ''}
		#mem_store[27] = m


		#mem_store[18] = m
		
		
		while True:
			
			process_counter = 0
			input_queue = list()		
			
			# Get work load and keep looking
			workload = WorkloadClass()
			workload_dict, file_list = workload.main(old_list)
			
			# Update the files that are done
			old_list.extend(file_list)
			
			# Function which gets workload elements and puts in in the queue
			for key, value in workload_dict.iteritems():
			
				input_queue.append((process_counter, key))
				#logging.debug("Process {} arrives.".format(process_counter))
				#logging.debug("Input Queue : {}".format([x[0] for x in input_queue if x]))
				
				process_counter += 1
				
			queue_process_id = [x[1] for x in input_queue if x]			
			queue_process_counter = [x[0] for x in input_queue if x]
			
			
			while input_queue:
				
				mem_store = self.wipe_old_process(workload_dict, mem_store, tt)
				
				if key in queue_process_counter:
				
					# if memory has space:
					queue_process_counter = [x[0] for x in input_queue if x]
					proc_id = queue_process_counter[0]
					workload_dict[proc_id]['proc_id'] = proc_id

					mem_store, mem_placed = self.memory_manager(workload_dict[proc_id], mem_store)


					#logging.debug("Process {} loaded into memory.".format(proc_id))
	
					# update input queue
					if input_queue:
						if mem_placed == True:
						
							workload_dict[proc_id]['st_time'] = tt.time_elapsed()


							input_queue.pop(0)
							
							logging.debug("Input Queue> : {}".format(input_queue))
						else:
						
							logging.error("Memory error for process {}.".format(proc_id))
							
							#process_time = tt.time_elapsed() - workload_dict['st_time']

							# Get the number of process which are running and wait forz some process 
							
							#while process_time < workload_dict['lifetime']:
							#	logging.error("Process {} ")
							
					
									
				
			logging.debug('Waiting...')
			time.sleep(2)
		
		
	def memory_manager(self, process_dict, mem_store):
		'''
		Main takes care of the counter
		'''
		#logging.debug("Starting Process : {}".format(process_dict))
	
		l = process_dict
						
		total_chunks = [int(x) for x in l['Chunks'] if x]
		empty_mem = self.get_slot_list(mem_store, 'empty')
		full_mem = self.get_slot_list(mem_store, 'full')
		diff = self.diff(mem_store)
		#self.chunk_in_interval(empty_mem)
	
		
		mem_placed = False
		
		# Variable first fit 
		if self.policy == 'VSP':
			
			# First fit
			if self.param == 'First Fit':
				mem_store, mem_placed = self.vsp_first_fit(mem_store, empty_mem, sum(total_chunks), l, diff)
				
					
			
			# Best fit
			if self.param == 'Best Fit':
				mem_store, mem_placed = self.vsp_best_fit(mem_store, empty_mem, sum(total_chunks), l, diff)
			
		# Paging
		if self.policy == 'PAG':
		
			pages = len(l['Chunks'])

			# Pick n number of pages 
			address = free_mem_address[:pages]

			for index, a in enumerate(address):
				l['index'] = a
				l['page'] = index
				mem_store[a] = l
				
			
			self.page_mem_check_log(mem_store)
	

		if self.policy == 'SEG':
			# We will go through the intervals and put the memory wherever we find
			mem_store[4] = {'Process ID': '1000', 'Total Chunks': '1', 'Arrival time': '0', 'Chunks': ['4', '8', '7'], 'lifetime': '16', 'hash': '533d1e2e5573aed87fe58f77c23dc01e515ceb76677678156e67e2d9', 'type':'', 'seg': '', 'st_time': 10, 'proc_id': '123123123'}
			
			# First fit
			if self.param == 'First Fit':
				mem_store, mem_placed = self.seg_first_fit(mem_store, empty_mem, total_chunks, l, diff)
				
			# Best fit
			if self.param == 'Best Fit':
				mem_store, mem_placed = self.seg_best_fit(mem_store, empty_mem, total_chunks, l, diff)
				
			
		self.mem_report(mem_store)
		key_list, interval_list, single, full_interval = self.get_free_intervals(mem_store)
		#print mem_store, interval_list
		#print full_interval
		return mem_store, mem_placed
	
	
	def wipe_old_process(self, full_dict, mem_store, tt):
		''' Wipe out process '''
		
		#print 'before wiping', mem_store
		for index, process_dict in full_dict.iteritems():
			
			logging.debug('Current Time = {}, Time since process({}) is running = {}, Life time = {}'.format(tt.time_elapsed(), process_dict['proc_id'], process_dict['st_time'], process_dict['lifetime']))
 
			if tt.time_elapsed() - int(process_dict['st_time']) >= int(process_dict['lifetime']):
				for index1, proc in mem_store.iteritems():
					if proc:
						if proc['proc_id'] == process_dict['proc_id']:
							logging.debug('Wiping out process {}'.format(process_dict['proc_id']))
						
							for a in self.get_procid_mem(mem_store, process_dict['proc_id']):
								mem_store[a] = ''
				
							self.mem_report(mem_store)
						
		return mem_store


	def get_procid_mem(self, mem_store, proc_id):
		''' Get keys from process_id '''
		temp = list()
		for k, v in mem_store.iteritems():
			if v != '':
				if int(v['proc_id']) == int(proc_id):
					temp.append(k)	
		return temp

	
	def get_free_intervals(self, mem_buffer):
	
		''' chunk up the free intervals '''
		
		single, temp = list(), list()
		interval_list = list()
		
		key_list = sorted([int(l) for l in mem_buffer.keys()])
		diff = key_list[1] - key_list[0]
		
		empty_slots = self.free_mem_chunks(mem_buffer)
		full_slots =  self.full_mem_chunks(mem_buffer)

		
		for index, element in enumerate(empty_slots):
			
			if index == 0:
				temp.append(element)
				
			
			if index > 0:
				
				if empty_slots[index-1] + diff == element:

					if empty_slots[index-1] not in temp:
						temp.append(empty_slots[index-1])
					temp.append(element)
					
					# Last index

					if index == len(empty_slots) -1:

						if empty_slots[index-1] + diff == element:
							if temp:
								if len(temp) == 1:
									single.append(temp)
								else:
									interval_list.append(temp)	
				else:
					if temp:
						if len(temp) == 1:
							single.append(temp)
						else:
							interval_list.append(temp)

					temp = list()
					
		full_interval = [item for sublist in interval_list for item in sublist]

		single = [x for x in empty_slots if x not in full_interval]

		return key_list, interval_list, single, full_interval


	def get_full_intervals(self, mem_buffer):
	
		''' chunk up the free intervals '''
		
		single, temp = list(), list()
		interval_list = list()
		
		key_list = sorted([int(l) for l in mem_buffer.keys()])
		diff = key_list[1] - key_list[0]
		
		full_slots =  self.full_mem_chunks(mem_buffer)

		
		for index, element in enumerate(full_slots):
			
			if index == 0:
				temp.append(element)
				
			
			if index > 0:

				
				if full_slots[index-1] + diff == element and \
					mem_buffer[full_slots[index-1]]['hash'] == \
						mem_buffer[full_slots[index]]['hash'] and \
						int(mem_buffer[full_slots[index-1]]['seg']) == int(mem_buffer[full_slots[index]]['seg']):

					if full_slots[index-1] not in temp:
						temp.append(full_slots[index-1])
					temp.append(element)
					
					# Last index

					if index == len(full_slots) - 1:

						if full_slots[index-1] + diff == element:
							if temp:
								if len(temp) == 1:
									single.append(temp)
								else:
									interval_list.append(temp)	
				else:
					if temp:
						if len(temp) == 1:
							single.append(temp)
						else:
							interval_list.append(temp)

					temp = list() 
					
		full_interval = [item for sublist in interval_list for item in sublist]

		single = [x for x in full_slots if x not in full_interval]

		return key_list, interval_list, single, full_interval


	
	def chunk_in_interval(self, interval_list):
	
		temp = dict()
		for element in interval_list:
			if len(element) == 1:
				pass
			
		
	
			
	def page_mem_check_log(self, mem_buffer):
	
		''' logging : 0-99: Process 1, Page 1 '''
		
		key_list, interval_list, single, full_interval = self.get_free_intervals(mem_buffer)
		diff = key_list[1] - key_list[0]
		
		for element in key_list:
			
			x = element
			v = mem_buffer[element]
			free, full = False, False
			
			if x in single or x in full_interval:
				free = True 
			else:
				full = True
			
			
			if full:
				logging.debug('{} - {} : Process {}, Page {}'.format(x, x+diff, v['Process ID'], ''))
				v['type'] = 'page {}'.format('')
				mem_buffer[x] = v

			if free:
			 	if x in single:
			 		logging.debug('{} - {} : No Process, Free Frame'.format(x, x+diff))
			 	else:
			 		for e in interval_list:
			 			if x in e:
			 				if x == e[0]:
			 					if len(e) == 1:
			 						logging.debug('{} - {} : No Process, Free Frame'.format(x, x+diff))
			 					else:
			 						logging.debug('{} - {} : No Process, Free Frame'.format(x, e[-1]))
			 			
			
	def get_slot_list(self, mem_buffer, status):
	
		''' logging : 0-99: Process 1, Page 1 '''
		
		if status == 'empty':
			key_list, interval_list, single, full_interval = self.get_free_intervals(mem_buffer)
		else:
			key_list, interval_list, single, full_interval = self.get_full_intervals(mem_buffer)
			
		diff = key_list[1] - key_list[0]
		final_list = list()
		
		for element in key_list:
			
			x = element
			v = mem_buffer[element]
			free, full = False, False
			
			if x in single:
				free = True 
			elif x in full_interval:
				full = True
			
			
			if full:
				for e in interval_list:
		 			if x in e:
		 				if x == e[0]:
		 					final_list.append(e)
				

			if free:
				final_list.append([x])
				
		return final_list			
			
			
	def create_mem_buffer(self):
	
		''' Create a memory buffer dictionary '''
		
		mem_buffer = dict()
		
		for chunk in range(0, int(self.mem_size), 1):
			mem_buffer[chunk] = ''
		
		return mem_buffer
		
	def free_mem_chunks(self, mem_buffer):
	
		''' Free memory chunks '''
		
		key_list = sorted([int(l) for l in mem_buffer.keys()])
		diff = key_list[1] - key_list[0]
			
		empty_slots = [f for f in key_list if not mem_buffer[f]]

		return empty_slots
		
	def full_mem_chunks(self, mem_buffer):
	
		''' Full memory chunks '''
		
		key_list = sorted([int(l) for l in mem_buffer.keys()])
		diff = key_list[1] - key_list[0]
			
		full_slots =  [c for c in key_list if mem_buffer[c] ]

		return full_slots

	def diff(self, mem_buffer):
				
		key_list = sorted([int(l) for l in mem_buffer.keys()])
		return key_list, key_list[1] - key_list[0]
		
		
	def vsp_first_fit(self, mem_store, empty_mem, total_chunks, data_dict, diff):
		
		# First fit
		mem_placed = False
		
		for element in empty_mem:
					
			empty_chunk = element[-1] - element[0]		
					
			if empty_chunk == 0 and total_chunks <= empty_chunk:
				logging.debug('Found chunk {} to place the process {}'.format(element, data_dict['Process ID']))
				data_dict['type'] = 'vsp-ff'
				data_dict['mem_address'] = [element[0]]
				mem_store[element[0]] = data_dict
				mem_placed = True
				break
						
			if total_chunks <= empty_chunk:
				logging.debug('Found chunk {} to place the process {}'.format(element, data_dict['Process ID']))

				for index, sub_chunk in enumerate(element):
					
					if index > total_chunks: 	
						break
						
					data_dict['type'] = 'vsp-ff'
					data_dict['mem_address'].append(sub_chunk)
					mem_store[sub_chunk] = data_dict
					
					
					
					#logging.debug('Storing in chunk {} : {}'.format(sub_chunk, mem_store[sub_chunk]))
				mem_placed = True
				logging.debug('Process ({}, Workload process ID {}) placed in memory chunks {}'.format(data_dict['proc_id'], data_dict['Process ID'], data_dict['mem_address']))
				break
							
		# Report if not placed 
		if not mem_placed:
			logging.error('Cannot find any chunk big enough for process({}, Workload process ID {}) size {}'.format(data_dict['proc_id'], data_dict['Process ID'], total_chunks))

		return mem_store, mem_placed	
		
			
	def vsp_best_fit(self, mem_store, empty_mem, total_chunks, data_dict, diff):
		
		# First fit
		mem_placed = False
		min_chunk, final_chunk = list(), list()
		chunk_list = [(x, x[-1]-x[0] + 1) for x in empty_mem]
		
		if chunk_list:
			min_chunk = [x[1] for x in chunk_list if x[1] > total_chunks]
	
			if final_chunk:
				final_chunk = [x for x in chunk_list if min(min_chunk) == x[1]]
		
		if not min_chunk:
			logging.error('Cannot find any chunk big enough for process({}) size {}'.format(data_dict['proc_id'], total_chunks))
			return mem_store, mem_placed
			
		
		min_chunk_size = min(min_chunk)

		for element in empty_mem:
					
			empty_chunk = element[-1] - element[0]		
					
			if empty_chunk == 0 and total_chunks <= diff:
				logging.debug('Found chunk {} to place the process {}'.format(element, data_dict['proc_id']))
				data_dict['type'] = 'vsp-bf'
				mem_store[element[0]] = data_dict
				mem_placed = True
				break
				
			#print final_chunk[0][1], final_chunk[0][0], element
			if final_chunk and total_chunks <= final_chunk[0][1] and final_chunk[0][0] == element:
				logging.debug('Found chunk {} to place the process {}'.format(element, data_dict['proc_id']))
				
				for index, sub_chunk in enumerate(element):
					
					if index > total_chunks: 	
						break
						
					data_dict['type'] = 'vsp-bf'
					mem_store[sub_chunk] = data_dict
					
				mem_placed = True
				break
							
		# Report if not placed
		if not mem_placed:
			logging.error('Cannot find any chunk big enough for process({}) size {}'.format(data_dict['proc_id'], total_chunks))

		return mem_store, mem_placed		
		
	
			
	def seg_first_fit(self, mem_store, empty_mem, total_chunks, data_dict, diff):
		
		# First fit
		mem_placed = False
		
		logging.debug('Total chunks : {}, empty mem : {}, '.format(total_chunks, empty_mem))
		
		for index, sub_chunk in enumerate(total_chunks):
			
			new_list = [item for sublist in self.get_slot_list(mem_store, 'empty') for item in sublist][:sub_chunk]
			logging.debug('Found SEG:{} chunks {} to place the process {}'.format(index, new_list, data_dict['proc_id']))			
			
			# Every time get an updated empty list and put the segments in there
			for sub_index in new_list:
						
				temp = dict()
				for a, v in data_dict.iteritems():
					temp[a] = v
				temp['seg'] = '{}'.format(index)
				temp['type'] = 'seg {}'.format(index)
				mem_store[sub_index] = temp
				
		mem_placed = True
							
		# Report if not placed
		if not mem_placed:
			logging.error('Cannot find any chunk big enough for process({}) size {}'.format(data_dict['proc_id'], total_chunks))

		return mem_store, mem_placed
		

	def seg_best_fit(self, mem_store, empty_mem, total_chunks, data_dict, diff):
		
		# First fit
		mem_placed = False
		
		logging.debug('Total chunks : {}, empty mem : {}, '.format(total_chunks, empty_mem))
		
		key_list, interval_list, single, full_interval = self.get_free_intervals(mem_store)
		print interval_list
		min_chunk, final_chunk = list(), list()
		chunk_list = [(x, x[-1]-x[0] + 1) for x in interval_list]
		print chunk_list
		if chunk_list:
			min_chunk = [x[1] for x in chunk_list if x[1] > total_chunks]
			print 'min chunk', min_chunk
			if min_chunk:
				final_chunk = [x for x in chunk_list if min(min_chunk) == x[1]]
			print 'final chunk', final_chunk
		if not min_chunk:
			logging.error('Cannot find any chunk big enough for process({}) size {}'.format(data_dict['proc_id'], total_chunks))
			return mem_store, mem_placed
			
		
		min_chunk_size = min(min_chunk)
		for index, sub_chunk in enumerate(total_chunks):
			
			new_list = [item for sublist in self.get_slot_list(mem_store, 'empty') for item in sublist]
			logging.debug('Found SEG:{} chunks {} to place the process {}'.format(index, new_list, data_dict['proc_id']))			
			
			# Every time get an updated empty list and put the segments in there
			for sub_index in new_list:
						
				temp = dict()
				for a, v in data_dict.iteritems():
					temp[a] = v
				temp['seg'] = '{}'.format(index)
				temp['type'] = 'seg {}'.format(index)
				mem_store[sub_index] = temp
				
		mem_placed = True
							
		# Report if not placed
		if not mem_placed:
			logging.error('Cannot find any chunk big enough for process({}) size {}'.format(data_dict['proc_id'], total_chunks))

		return mem_store, mem_placed
				
		
	def mem_report(self, mem_buffer):
		""" Get Memory report """
		
		logging.debug("------------------- MEMORY REPORT -----------------------")
		
		empty_mem = self.get_slot_list(mem_buffer, 'empty')
		full_mem = self.get_slot_list(mem_buffer, 'full')
				
		key_list, diff = self.diff(mem_buffer)

		for element in key_list:
			
			x = element
			v = mem_buffer[element]
			free, full = False, False
			
			if x in [item for sublist in empty_mem for item in sublist]:
				free = True 
			else:
				full = True
			
			
			if full:
				for e in full_mem:
			 		if x in e:
			 			if x == e[0]:
			 				if len(e) == 1:
			 					logging.debug('{} - {} : Process {} , {}'.format(x, x+diff, v['proc_id'], v['type']))
			 				else:
			 					logging.debug('{} - {} : Process {} , {}'.format(x, e[-1], v['proc_id'], v['type']))
				
				


			if free:
			 	for e in empty_mem:
			 		if x in e:
			 			if x == e[0]:
			 				if len(e) == 1:
			 					logging.debug('{} - {} : No Process, Free Frame'.format(x, x+diff))
			 				else:
			 					logging.debug('{} - {} : No Process, Free Frame'.format(x, e[-1]))
		
		logging.debug("------------------- MEMORY REPORT -----------------------")
		
		
		
		
def parser():

    	parser = argparse.ArgumentParser()
	parser.add_argument('-s', "--single", help='Single Thread')
	parser.add_argument('-d', "--dir", help='Directory to be mounted', required=True)
	parser.add_argument('-m', "--mountpt", help='Mount point', required=True)
	parser.add_argument('-l', "--level", help='level DB', required=True)

	args = parser.parse_args()
	
	# Parse the arguments
	mountpoint = args.mountpt
	root = args.dir
	single = args.single
	lvldb = args.level


if __name__ == '__main__':

	
	# Enable logging
	logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s  ')
	logging.info('Start Memory manager')
	
	user_input = InputClass()
	logging.debug('Starting MM for memory size = {}, Policy : {} , {}'.format(user_input.mem_size, user_input.policy, user_input.param))
	
	mm = MM(user_input)
	#mm.main()
	mm.gand()
	
