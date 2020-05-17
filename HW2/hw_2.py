import numpy as np
import pandas as pd

class Load_Balancer:

	def __init__(self, beta,number_of_servers,number_of_task):
		self.number_of_task=number_of_task
		self.processing = {t:self.processing_time(beta=beta) for t in range(self.number_of_task)}
		self.arrival_times = self.inter_arrival_times(q=3 / 5, T0=1, mean_Y=10, N=self.number_of_task)
		self.servers = {i: {"que": [], "proces": 0 ,"processing_task":0} for i in range(0, number_of_servers)}
		self.task = {i:{"sum":0,"entrance_time": 0, "quiting_time": 0,
						"processing_time":self.processing[i],"arrival_time":self.arrival_times[i],
						"server":0} for i in range(len(self.processing))}
		self.global_time=0
		self.current_task=0
		self.next_arriving_time=0

	def inter_arrival_times(self,q, T0, mean_Y, N):
		inter_times = {}
		for i in range(N):
			r = np.random.random()
			if r <= q:
				T = T0
			else:
				T = T0 - mean_Y * np.log(np.random.random())
			inter_times[i] = int(round(T))
		return (inter_times)

	def processing_time(self,beta):
		expected_service_time = beta * 2
		r = np.random.random()
		confront_min = []
		confront_min.append(round(np.log(r) * np.log(r) * beta))
		confront_min.append(100 * expected_service_time)
		min_temp = min(confront_min)
		confront_max = []
		confront_max.append(1)
		confront_max.append(int(min_temp))
		weibell_rv = max(confront_max)
		return (weibell_rv)

	def iterate_one_task(self):
		min_que_holder = float("+inf")
		min_que_server = 0

		for server in self.servers:
			is_processing = 1 if self.servers[server]["proces"] else 0
			if len(self.servers[server]["que"]) + is_processing < min_que_holder:
				min_que_holder = len(self.servers[server]["que"])+is_processing
				min_que_server = server
		self.task[self.current_task]["server"]=min_que_server
		self.servers[min_que_server]["que"].append(self.current_task)
		if self.servers[min_que_server]["proces"] == 0:
			item_in_proces=self.servers[min_que_server]["que"].pop(0)
			self.servers[min_que_server]["proces"]=self.processing[item_in_proces]
			self.servers[min_que_server]["processing_task"] = item_in_proces

		self.task[self.current_task]["entrance_time"] = self.global_time
		self.current_task += 1
		try:
			self.next_arriving_time=self.arrival_times[self.current_task]
		except:
			print("Last Element")
			self.next_arriving_time=self.arrival_times[self.current_task -1 ]

	def iterate_over_global_time(self):
		for time in range(self.next_arriving_time):
			self.global_time += 1
			for server in self.servers:
				if self.servers[server]["proces"] > 0:
					self.servers[server]["proces"] -= 1
					if self.servers[server]["proces"] == 0:
						self.task[self.servers[server]["processing_task"]]["quiting_time"]=self.global_time
						if len(self.servers[server]["que"]) > 0 :
							item_in_proces=self.servers[server]["que"].pop(0)
							self.servers[server]["proces"] = self.processing[item_in_proces]
							self.servers[server]["processing_task"] = item_in_proces

	def finishing_all_the_task(self):
		for server in self.servers:
			initial_global_for_each_server=self.global_time
			while len(self.servers[server]["que"]) :
				print(self.servers[server]["proces"])
				if self.servers[server]["proces"] > 0:
					initial_global_for_each_server += self.servers[server]["proces"]
					self.task[self.servers[server]["processing_task"]]["quiting_time"] = initial_global_for_each_server
					next_item_in_que=self.servers[server]["que"].pop(0)
					self.servers[server]["processing_task"]=next_item_in_que
					self.servers[server]["proces"]=self.processing[next_item_in_que]
			initial_global_for_each_server += self.servers[server]["proces"]
			self.task[self.servers[server]["processing_task"]]["quiting_time"] = initial_global_for_each_server
			self.servers[server]["processing_task"] = "nothing_left"
			self.servers[server]["proces"] = "idle"


	def	main(self):
		for i in range(self.number_of_task):
			load_balance.iterate_one_task()
			load_balance.iterate_over_global_time()

		load_balance.finishing_all_the_task()



if __name__ == "__main__":
	load_balance=Load_Balancer(beta=40,number_of_servers=20,number_of_task=100000)
	load_balance.main()
	print("Total Amount of Time For Processing All Task",load_balance.global_time)
	print("Total Time of All Task",sum(load_balance.processing.values()))
	df=pd.DataFrame.from_dict(load_balance.task,orient="index")
	df["sum"]=df["quiting_time"]-df["entrance_time"]
	df.to_csv("output1.csv")
