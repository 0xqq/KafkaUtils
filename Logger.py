#!/usr/bin/python
#-*- coding:utf8 -*-
import logging
from logging.handlers import RotatingFileHandler
import threading
import ConfigParser


class LogSignleton(object):

    def __init__(self, log_file, log_config): pass


    def __new__(cls, log_file, log_config):
        mutex=threading.Lock()
        mutex.acquire() # 上锁，防止多线程下出问题

        if not hasattr(cls, 'instance'):
            cls.instance = super(LogSignleton, cls).__new__(cls)
            config = ConfigParser.ConfigParser()
            config.read(log_config)
            cls.instance.log_filename = log_file
            cls.instance.max_bytes_each = int(config.get('LOGGING', 'max_bytes_each'))
            cls.instance.backup_count = int(config.get('LOGGING', 'backup_count'))
            cls.instance.fmt = config.get('LOGGING', 'fmt')
            cls.instance.log_level_in_console = int(config.get('LOGGING', 'log_level_in_console'))
            cls.instance.log_level_in_logfile = int(config.get('LOGGING', 'log_level_in_logfile'))
            cls.instance.logger_name = config.get('LOGGING', 'logger_name')
            cls.instance.console_log_on = int(config.get('LOGGING', 'console_log_on'))
            cls.instance.logfile_log_on = int(config.get('LOGGING', 'logfile_log_on'))
            cls.instance.logger = logging.getLogger(cls.instance.logger_name)
            cls.instance.__config_logger()

        mutex.release()

        return cls.instance

 

    def get_logger(self):
        return  self.logger

 

    def __config_logger(self):
        ''' 设置日志格式 '''
        fmt = self.fmt.replace('|','%')
        formatter = logging.Formatter(fmt)
 
         ''' 开启控制台日志 '''
        if self.console_log_on == 1:
            console = logging.StreamHandler()
            console.setFormatter(formatter)
            self.logger.addHandler(console)
            self.logger.setLevel(self.log_level_in_console)

 
        ''' 开启写入文件日志 '''
        if self.logfile_log_on == 1:
            rt_file_handler = RotatingFileHandler(self.log_filename, maxBytes=self.max_bytes_each, backupCount=self.backup_count)
            rt_file_handler.setFormatter(formatter)
            self.logger.addHandler(rt_file_handler)
            self.logger.setLevel(self.log_level_in_logfile)