import logging
from logging.handlers import RotatingFileHandler

log = logging.getLogger('collate_warc')
formatter = logging.Formatter('%(asctime)s-%(process)d - %(levelname)s - %(message)s')
home_path = os.path.expanduser("~")
home_path = "/SSD/"
os.makedirs(os.path.join(home_path, 'logs'), exist_ok=True)
handler = RotatingFileHandler(os.path.join(home_path, "logs", "collate_warc_activity_%s.log" % (time.strftime("%Y%m%d"))),
                              maxBytes=750*1024*1024, backupCount=3)
handler.setFormatter(formatter)
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(formatter)
log.setLevel(logging.INFO)
log.addHandler(handler)
# log.addHandler(stream_handler)

os.makedirs(os.path.join(home_path, 'data_logs'), exist_ok=True)
data_log = logging.getLogger('data_collate_warc')
data_formatter = logging.Formatter('%(message)s')
data_handler = RotatingFileHandler(os.path.join(home_path, 'data_logs', "data_records_%s_%s.json" % (time.strftime("%Y%m%d"), os.getpid())), maxBytes=500*1024*1024, backupCount=3)
data_handler.setFormatter(data_formatter)
data_log.setLevel(logging.DEBUG)
data_log.addHandler(data_handler)



