#!/usr/bin/env python

from zeroconf import Zeroconf, ServiceInfo
import RPi.GPIO as GPIO
import netifaces, socket
import logging, logging.config, sys, os, argparse
import json, shelve
import fnmatch
import RPi.GPIO as GPIO
import pika

SERVER_NAME = "Team 23's Message Bottle Server"
VIRTUAL_HOST = "/bottle"
EXCHANGE_NAME = "pebble"
ROUTING_KEY = "actions"
MSG_DB_FILE = "bottle.msgs"
MSG_DB = shelve.open(MSG_DB_FILE, writeback=True)
GPIO_PINS = [11,13,15,12]
GPIO_EN = False

COMMAND_DESC = "RabbitMQ-based server for distribution and storage of text messages"
DEFAULT_LOGFILE = "bottle.log"
LOG_FMT = logging.Formatter(fmt="%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%b %d %H:%M:%S")
LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

# Returns a server name to use for zeroconf advertisement
def getServiceName():
	return SERVER_NAME + "._http._tcp.local."

# Returns the IP of either the eth0 or wlan0 interfaces
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

# Check if this program is running in the FG
def isRunningInFg():
	return os.getpgrp() == os.tcgetpgrp(sys.stdout.fileno())	

# Process a command message from a client
def processCmd(ch, method, properties, body):
	# Attempt to parse the message
	try:
		msg = json.loads(body)
	except ValueError:
		LOG.warn("Could not parse message \'%s\'" % (body,))
		return
	
	# If message does not specify an action or destination it is invalid
	if "Action" not in msg or "Dest" not in msg:
		LOG.info("Recieved invalid message \'%s\'" % (msg,))
		return
	
	# Push a message to the server
	if msg["Action"].lower() == "push":
		LOG.info("PUSH request recieved")
		# Initialize the message store
		if "msgs" not in MSG_DB:
			MSG_DB['msgs'] = []

		# Add the message, send a success response, and update the LED's
		MSG_DB['msgs'].append(msg)
		sendMsg(msg["Dest"], json.dumps({"Status": "success"}))
		updatePins()
	# Pull a message from the server
	elif msg["Action"].lower() == "pull" or msg["Action"].lower() == "pullr":
		# No messages available, send failure response
		if "msgs" not in MSG_DB or len(MSG_DB["msgs"]) == 0:
			sendMsg(msg["Dest"], json.dumps({"Status": "failed", "Reason": "No messages available"}))
		else:
			# Otherwise filter messages by queries and send one
			msgList = MSG_DB["msgs"]
			if "Query_Author" in msg and msg["Query_Author"]:
				msgList = filter(lambda storedMsg: "Author" in storedMsg and fnmatch.fnmatch(storedMsg["Author"], msg["Query_Author"]), msgList)
			if "Query_Age" in msg and msg["Query_Age"]:
				msgList = filter(lambda storedMsg: "Age" in storedMsg and fnmatch.fnmatch(str(storedMsg["Age"]), str(msg["Query_Age"])), msgList)
			if "Query_Subject" in msg and msg["Query_Subject"]:
				msgList = filter(lambda storedMsg: "Subject" in storedMsg and fnmatch.fnmatch(storedMsg["Subject"], msg["Query_Subject"]), msgList)
			if "Query_Message" in msg and msg["Query_Message"]:
				msgList = filter(lambda storedMsg: "Message" in storedMsg and fnmatch.fnmatch(storedMsg["Message"], msg["Query_Message"]), msgList)

			# All messages failed the queries
			if len(msgList) == 0:
				sendMsg(msg["Dest"], json.dumps({"Status": "failed", "Reason": "No messages available"}))
			else:
				# Get a message that matches and send it
				pulledMsg = msgList[0]
				if msg["Action"].lower() == "pull":
					# Delete message from server
					LOG.info("PULL request recieved")
					MSG_DB["msgs"].remove(pulledMsg)
					updatePins()
				else:
					LOG.info("PULLR request recieved")
				sendMsg(msg["Dest"], json.dumps(pulledMsg))
	else:
		# Command is not recognized
		LOG.info("Recieved message with unknown command \'%s\'" % (msg["Action"]))
		sendMsg(msg["Dest"], json.dumps({"Status": "failed", "Reason": "Unknown command \'%s\'" % msg["Action"]}))

# Helper function for sending a message to a client's queue		
def sendMsg(dest, msg):
	LOG.info("Sending message %s to %s" %(msg, dest,))
	# Open up another temporary connection to send
	tempConn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', virtual_host=VIRTUAL_HOST))
	tempChan = tempConn.channel()
	tempChan.queue_declare(queue=str(dest), passive=True)
	tempChan.basic_publish(exchange="", routing_key=str(dest), body=msg)
	tempConn.close()

def updatePins():
	numMsgs = len(MSG_DB["msgs"]) if "msgs" in MSG_DB else 0
	LOG.info("%d messages stored on this server" % numMsgs)
	if GPIO_EN:
		for i, pin in enumerate(GPIO_PINS):
			GPIO.output(pin, numMsgs & 2**i)

if __name__ == "__main__":
	# Attempt to configure GPIO
	GPIO.setwarnings(False)
	GPIO.setmode(GPIO.BOARD)
	try:
		GPIO.setup(GPIO_PINS, GPIO.OUT)
		GPIO_EN = True
	except RuntimeError:
		pass	

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

	# Warn if the GPIO pins couldn't be configured
	if not GPIO_EN:
		LOG.warn("Could not configure GPIO pins, make sure this srunning with superuser priveleges!")
	updatePins()

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
		shelve.close(MSG_DB_FILE)

		LOG.info("Closing connection with RabbitMQ")
		if conn is not None:
			conn.close()
		
		LOG.info("Unregistering server")
		zeroconf.unregister_service(zeroconf_info)
		zeroconf.close()
	
		LOG.info("Shutdown complete!")
