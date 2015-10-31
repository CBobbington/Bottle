from zeroconf import Zeroconf, ServiceInfo
import RPi.GPIO as GPIO
import netifaces, socket
import logging, logging.config, sys, os, argparse
import json, shelve
import pika

SERVER_NAME = "Team 23's Message Bottle Server"
VIRTUAL_HOST = "/bottle"
EXCHANGE_NAME = "pebble"
ROUTING_KEY = "actions"
MSG_DB = shelve.open("bottle.msgs", writeback=True)

COMMAND_DESC = "RabbitMQ-based server for distribution and storage of text messages"
DEFAULT_LOGFILE = "bottle.log"
LOG_FMT = logging.Formatter(fmt="%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%b %d %H:%M:%S")
LOG = logging.getLogger()
LOG.setLevel(logging.DEBUG)

def getServiceName():
	return SERVER_NAME + "._http._tcp.local."

def getServiceIP():
	wlanIfaceAddrs = netifaces.ifaddresses('wlan0')
	ethIfaceAddrs = netifaces.ifaddresses('eth0')

	# Try getting IP from wlan0 first, then eth0 if we can't
	if netifaces.AF_INET in wlanIfaceAddrs and "addr" in wlanIfaceAddrs[netifaces.AF_INET][0]:
		return wlanIfaceAddrs[netifaces.AF_INET][0]["addr"], "wlan0"
	elif netifaces.AF_INET in ethIfaceAddrs and "addr" in ethIfaceAddrs[netifaces.AF_INET][0]:
		return ethIfaceAddrs[netifaces.AF_INET][0]["addr"], "eth0"
	else:
		# Couldn't get IP from either, return None
		return None, None

def isRunningInFg():
	return os.getpgrp() == os.tcgetpgrp(sys.stdout.fileno())	

def processCmd(ch, method, properties, body):
	try:
		msg = json.loads(body)
	except ValueError:
		LOG.warn("Could not parse message \'%s\'" % (body,))
		return
	
	if "Action" not in msg or "Dest" not in msg:
		LOG.info("Recieved invalid message \'%s\'" % (msg,))
		return
	
	if msg["Action"].lower() == "push":
		LOG.info("PUSH request recieved")
		if "msgs" not in MSG_DB:
			MSG_DB['msgs'] = []
		MSG_DB['msgs'].append(msg)
		sendMsg(msg["Dest"], json.dumps({"Status": "success"})) 
	elif msg["Action"].lower() == "pull" or msg["Action"].lower() == "pullr":
		toReturn = {}
		if "msgs" not in MSG_DB or len(MSG_DB["msgs"]) == 0:
			sendMsg(msg["Dest"], json.dumps({"Status": "failed"}))
		else:
			if msg["Action"] == "pull":
				LOG.info("PULL request recieved")
				sendMsg(msg["Dest"], json.dumps(MSG_DB["msgs"].pop(0)))
			else:
				LOG.info("PULLR request recieved")
				sendMsg(msg["Dest"], json.dumps(MSG_DB["msgs"][0]))
	else:
		LOG.info("Recieved message with unknown command \'%s\'" % (msg["Action"],))
		sendMsg(msg["Dest"], json.dumps({"Status": "failed"}))
		
def sendMsg(dest, msg):
	LOG.info("Sending message %s to %s" %(msg, dest,))
	tempConn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', virtual_host=VIRTUAL_HOST))
	tempChan = tempConn.channel()
	tempChan.queue_declare(None, queue=str(dest), passive=True)
	tempChan.basic_publish(exchange="", routing_key=str(dest), body=msg)
	tempConn.close()

if __name__ == "__main__":	
	# Configure logging
	parser = argparse.ArgumentParser(description=COMMAND_DESC)
	parser.add_argument("-l", "--enable_fg_logging", action='store_true', help="Enable logging when running in the foreground")
	parser.add_argument("-lf", "--logfile", default=DEFAULT_LOGFILE, type=str, help="File to use for log output")
	args = parser.parse_args()
	
	# Enable output to console when program is running in fg
	if isRunningInFg():
		streamHandler = logging.StreamHandler()
		streamHandler.setLevel(logging.DEBUG)
		streamHandler.setFormatter(LOG_FMT)
		LOG.addHandler(streamHandler)
	
	# Enable logging to file if running in bg or log flag is set
	if not isRunningInFg() or args.enable_fg_logging:
		fileHandler = logging.FileHandler(args.logfile)
		fileHandler.setLevel(logging.DEBUG)
		fileHandler.setFormatter(LOG_FMT)
		LOG.addHandler(fileHandler)
		LOG.info("Outputting to logfile \'%s\'" % (args.logfile,))

	LOG.info("Starting server...")

	# Configure queues
	conn = pika.BlockingConnection(pika.ConnectionParameters(host="localhost", virtual_host=VIRTUAL_HOST))	
	chan = conn.channel()

	queueResult = chan.queue_declare(exclusive=True)
	if queueResult is None:
		LOG.error("Could not create queue")
		exit(1)
	else:
		LOG.info("Created queue \'%s\'" % (queueResult.method.queue,))
	
	LOG.info("Using exchange \'%s\'" % (EXCHANGE_NAME,))
	chan.exchange_declare(exchange=EXCHANGE_NAME, type="direct", auto_delete=True)
	chan.queue_bind(exchange=EXCHANGE_NAME, queue=queueResult.method.queue, routing_key=ROUTING_KEY)
	chan.basic_consume(processCmd, queueResult.method.queue, no_ack=True, exclusive=True)

	# Configure zeroconf to broadcast this service
	zeroconf = Zeroconf()

	server_ip, ifaceName = getServiceIP()
	if server_ip is None:
		LOG.error("Could not determine server IP")
		exit(1)
	else:
		LOG.info("Broadcasting with IP %s (%s)" % (server_ip, ifaceName))
	
	zeroconf_info = ServiceInfo("_http._tcp.local.",
		getServiceName(),
		socket.inet_aton(server_ip),
		5672, 0, 0,
		{"exchange_name": EXCHANGE_NAME, "routing_key": ROUTING_KEY, "virtual_host": VIRTUAL_HOST},
		None)
	
	try:
		zeroconf.register_service(zeroconf_info)
	except Zeroconf.NonUniqueNameException:
		LOG.warn("Service with name \'%s\' already broadcasting on this network!" % (getServiceName(),))

	try:
		while True:
			LOG.info("Now waiting for messages on %s" % (queueResult.method.queue,))
			chan.start_consuming()
	except KeyboardInterrupt:
		chan.stop_consuming()
		pass
	finally:
		LOG.info("Shutting down server...")
		LOG.info("Closing connection with RabbitMQ")
		if conn is not None:
			conn.close()
		
		LOG.info("Unregistering server")
		zeroconf.unregister_service(zeroconf_info)
		zeroconf.close()
	
		LOG.info("Shutdown complete!")
