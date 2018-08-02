#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
import sys
import getopt
import time
import yaml
import threading
import multiprocessing
from multiprocessing import Process, Manager
from ConfluentKafkaConsumer import KafkaConsumer


# 获取可用的后缀序号
def get_available_num(prefix, suffix, output_dir):
	exists_file_num = []
	available_num = 0
	files = os.listdir(output_dir)

	for file in files:
		iprefix = file.split('_')[0] + '_' + file.split('_')[1] + '_'

		if iprefix == prefix:
			file_num = int(file.split('_')[2].split(suffix)[0])
			exists_file_num.append(file_num)

	while available_num in exists_file_num: available_num += 1

	return available_num



# 进行超时监控
def close_write(file_obj_dict, lock):
	while(1):

		time.sleep(10)
		lock.acquire()

		if file_obj_dict:

			for key,value in file_obj_dict.items():
				if (time.time() - file_obj_dict[key][2]) >= 5:
					print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + '  ' + 'Timeout close write ' + key
					file_obj_dict[key][0].close()
					del file_obj_dict[key]

		lock.release()



# 开启进程进行消费
def data_consume(broker, port, topics, consumer_group, output_dir, columns):

	try:
		# {文件名mark：[文件操作对象，写入行数，最后写入ms]}
		file_obj_dict = {}

		lock = threading.Lock()
		pid = str(os.getpid())

		print 'Strat data_consume process-'+ str(os.getpid())

		consumer = KafkaConsumer(broker, port, consumer_group)
		messages = consumer.consumeFromKafka(topics)
		# 开启监控超时线程
		threading.Thread(target=close_write,args=(file_obj_dict,lock,)).start()

	except Exception,e:
		print repr(e)
		sys.exit()

		

	# 消费并写入文件
	if messages:

		for message in messages:

			try:
				table_name = (message.split('|')[0]).lower()
				fields = message.split('|',1)[1]
				# lock.acquire()
				file_name = ''
				prefix = table_name + '_' + pid + '_'
				suffix = '.txt'

				if file_obj_dict:
					for key in file_obj_dict:
						if key.startswith(prefix):
							file_name = key + suffix
							break

				if file_name == '':
					num = get_available_num(prefix, suffix, output_dir)
					file_name = prefix + str(num) + suffix
				
				file_path = os.path.join(output_dir, file_name)

				identifier = file_name.split(suffix)[0]

				if os.path.exists(file_path) == False:
					os.mknod(file_path)
					file_obj_dict[identifier] = []
					file_obj_dict[identifier].append(open(file_path,'w'))
					file_obj_dict[identifier].append(0)
					file_obj_dict[identifier].append(0)
				
				file_obj_dict[identifier][0].write(fields+'\n')
				file_obj_dict[identifier][1] += 1
				file_obj_dict[identifier][2] = time.time()

				if file_obj_dict[identifier][1] == columns:
					file_obj_dict[identifier][0].close()
					del file_obj_dict[identifier]
					print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + '  ' + file_name

				# lock.release()

			except Exception,e:
				print repr(e)
				sys.exit()

def Usage():
    print 'usage:'
    print './multiprocessConsumer.py -c [conf]'

def main():

	if len(sys.argv) != 3:
		Usage()
		sys.exit()

	try:
		opts, args = getopt.getopt(sys.argv[1:], "c:")
	except getopt.GetoptError:
		Usage()
		sys.exit()

	for name, value in opts:
		if name == "-c": conf = value

	if os.path.exists(conf) == False:
		print 'No such file or directory: %s'%conf
		sys.exit()

	stream = file(conf, 'rt')
	data = yaml.load(stream)

	# 读取配置
	broker = data['broker']
	port = data['port']
	topics = data['topics']
	consumer_group = data['group']
	output_dir = data['output']
	process_num = data['processes']
	columns = data['columns']
	

	if os.path.exists(output_dir) == False:
		print 'No such file or directory: %s'%output_dir
		sys.exit()


	'''开启多进程进行消费, 进程数小于等于topic的partition
	每个进程中开启一个子线程, 监控消费超时, 关闭文件对象'''
	pool = multiprocessing.Pool(processes=process_num)
	for i in xrange(0,process_num):
		pool.apply_async(data_consume, [broker, port, topics, consumer_group, output_dir, columns])

	pool.close()
	pool.join()

if ( __name__ == "__main__"):
	main()