#!/usr/bin/python

import time
import thread
import asyncore
from math import fabs
#import daemon
from plugwise import *
import sys
import ConfigParser


MAX_MESSAGE_LENGTH = 1024

DEFAULT_SERIAL_PORT = "/dev/ttyUSB0"

class EventCircle(Circle):
	LastPowerValue = -99999
	Power = -99999
	LastReadAt = 0
	LastUpdate = 0
	Threshhold = 5.0
	Threshhold2 = 1.0
	History = 0.0
	History_until = None

	def __init__(self,mac,stick):
		Circle.__init__(self,mac,stick)
		self.mname = mac
		self.error = 0
		self.online = True
		self.nskips = 0
		self.buffer=[0,0,0,0,0,0,0,0,0,0]
		self.bufferPos = 0
		return

	def SetThreshhold(self,Value):
		self.Threshhold = Value

	def StoreToBuffer(self,power):
		#Store history
		self.buffer[self.bufferPos]=power
		self.bufferPos += 1
		if self.bufferPos >= len(self.buffer):
			self.bufferPos = 0
		return

	def GetBufferMaxMin(self):
		copy = []
		copy.extend(self.buffer)
		copy.sort()
		return (copy[-1],copy[0])

	def GetChange(self):
		if self.online==False and self.nskips < 30:
			self.nskips += 1
			return None

		self.nskips = 0

		try:
			pulse_1s, pulse_8s, pulse_60min = self.get_pulse_counters()
			req_time = time.time()
			corrected_pulses = self.pulse_correction(pulse_1s)
			Power = self.pulses_to_kWs(corrected_pulses)*1000

			self.LastReadAt = req_time

			#Time since last update
			tsl = req_time - self.LastUpdate

			#Change since last update.
			change = fabs(Power - self.LastPowerValue)

			AvrgLast2 = (Power + self.Power)/2

			#Change since last check.
			change_check = fabs(Power - self.Power)


			#Last X seconds man and min values.
			(max,min) = self.GetBufferMaxMin()
			self.StoreToBuffer(Power)

			#Debug
			#if self.mname == "000D6F000072A183":
			#	print (max,min)
			#	print self.buffer
			#	print self.LastPowerValue

			#Fix for poor resolution on low scale
			#if Power < 5.0:
			#	Power = self.AverageLast(3)


			if (change > self.Threshhold) or (Power < (min-0.5) ) or (Power > (max+0.5)) or (max == min and self.LastPowerValue != min):
				#False zeros depeding on low load. 
				if Power == 0.0 and self.Power != 0.0:
					self.Power = Power
					return None
				self.LastUpdate = req_time
				Energy = self.GetTotalEnergy(pulse_60min)
				self.LastPowerValue = Power 
				self.error = 0
				self.online = True
				return (req_time,Power,Energy)

			self.Power = Power

		except ValueError:
			self.error +=1
			print "Error reading plug %s  %i times"%(self.mname,self.error)
		except exceptions.TimeoutException:
			self.error +=1
			print "Timeout reading plug %s  %i times"%(self.mname,self.error)

		if self.error == 3:
			self.online = False
			return -1

		#If no change return nothing       	
		return None

	def GetTotalEnergy(self,last_hour_pulses = None):
		#Get count if needed.
		if last_hour_pulses == None:
			_, _, last_hour_pulses = self.get_pulse_counters()
		corrected_pulses = self.pulse_correction(last_hour_pulses,3600)
		Energy = self.pulses_to_kWs(corrected_pulses)

		self.UpdateEnergyHistory()

		return Energy + self.History	

	def UpdateEnergyHistory(self):
		return



class PlugwiseEventHandler(mosquitto.Mosquitto,Stick):

	def __init__(self,ip = "localhost", port = 1883, clientId = "Plugwise2MQTT", user = "driver", password = "1234", prefix = "Plugwise", device = "/dev/ttyUSB0"):

		mosquitto.Mosquitto.__init__(self,clientId)

		self.prefix = prefix
		self.ip = ip
    		self.port = port
    		self.clientId = clientId
		self.user = user
    		self.password = password
    		
    		if user != None:
    			self.username_pw_set(user,password)

		self.will_set( topic =  "system/" + self.prefix, payload="Offline", qos=1, retain=True)
    		print "Connecting"
    		self.connect(ip,keepalive=10)
    		self.subscribe(self.prefix + "/#", 0)
    		self.on_connect = self.mqtt_on_connect
    		self.on_message = self.mqtt_on_message
    		self.publish(topic = "system/"+ self.prefix, payload="Online", qos=1, retain=True)
    		
    		self.Updates = {}

		#thread.start_new_thread(self.ControlLoop,())	
		self.loop_start()
		
		
		#Plugwise stuff
		Stick.__init__(self,device,2)
		
		#The list of plugs. 
    		self.Plugs = []
    		
    		#This is the sum of several sensor feeds. 
		self.VirtualMeters = []
						
		return

	def AddVirtualMeter(self,Name,Meters):
		self.VirtualMeters.append(VirtualMeter(Name,Meters))
		print "Added virtual meter: %s" % Name
		return 

        def AddPlug(self,name,mac):
        	Plug = EventCircle(mac, self)
		Plug.Name = name
        	self.Plugs.append(Plug)
		print "Added sensor: %s" % Plug.Name
		return
    		
    	def mqtt_on_connect(self, selfX,mosq, result):
    		print "MQTT connected!"
    		self.subscribe(self.prefix + "/#", 0)
    
  	def mqtt_on_message(self, selfX,mosq, msg):
    		print("RECIEVED MQTT MESSAGE: "+msg.topic + " " + str(msg.payload))
    	
    		return
    	
    	def PollAndSendEvents(self,delay):

		while(self.running):
			start = time.time()
			Events = self.FindEvents()

			#for event in Events:
				#print event
			#	self.SendEventDirectToSocket(event[1],"METER_EVENT",event[0],str(event[2]))


			wait_time = delay - (time.time() - start)
			#print wait_time
			if wait_time > 0:
				time.sleep(wait_time)
			#print "tick!"

		return
	
		
	def FindEvents(self):
		Events = []

		#Total power fix
		TotalPower =0
		Count = 0
		TotalTime = 0
		starttime = time.time()		

		for plug in self.Plugs:
			change = plug.GetChange()

			if change == -1:
				#Events.append((plug.Name,time.time(),"Offline","",""))
				msg = json.dumps({"time":time.time(),"value":"Offline"})

				topic = self.prefix + "/"+ plug.name + "/status"
                		self.publish(topic,msg,1)
				continue

			if change == None:
				continue


			#Events.append((plug.Name,change[0],change[1],change[2]))
			msg = json.dumps({"time":time.time(),"value":change[0]})

			topic = self.prefix + "/"+ plug.name + "/power" 
                	self.publish(topic,msg,1)

			for VirtualMeter in self.VirtualMeters:
				VirtualMeter.Update(plug.Name,change[1])
				

		for VirtualMeter in self.VirtualMeters:
			change = VirtualMeter.GetChange()
			if change == None:
				continue
			
			#Events.append((VirtualMeter.Name,time.time(),change,"0","0"))
			msg = json.dumps({"time":time.time(),"value":change})

			topic = self.prefix + "/"+ VirtualMeter.Name + "/power" 
                	self.publish(topic,msg,1)

		#Print the time it took
		looptime = time.time() - starttime
		if looptime > 2:
			print "WARNING! Loop time %f s"	% (looptime)	

		return 


                


if __name__ == '__main__':
	
	
	#Load config file... 
	
	try:
		ConfigFile = sys.argv[1]
	except:
		ConfigFile = ""

	try:
		f = open(ConfigFile,"r")
		f.close()
	except:
		print "No valid config file supplied as argument!"
		
	config = ConfigParser.RawConfigParser(allow_no_value=True)
	config.read(ConfigFile)
	
	
	#Load basic config. 
	Name = config.get("PlugwiseOptions","Name")
	ip = config.get("MQTTServer","Address")
	port = config.get("MQTTServer","Port")
	user = config.get("MQTTServer","User")
	password = config.get("MQTTServer","Password")
	prefix = config.get("MQTTServer","Prefix")
	device = config.get("PlugwiseOptions","PlugwisePort")
	
	
	#Create eventhandler
	EventHandler = PlugwiseEventHandler(ip, port, Name, user, password, prefix, device)

	#Add sensors
	Sensors = config.items("PlugwiseSensors")

		for Sensor in Sensors:
			mac = Sensor[0]
			EventHandler.AddPlug(Sensor[1],mac)

	#Add virtualmeters i.e. the sum of several sensor feeds. 
	for i in range(1,30):
		SectionName = "VirtualPlugwiseMeter%i"%i

		if config.has_section(SectionName):
			items = config.items(SectionName)

			for item in items:
				if item[0] == "name":
					Name = item[1]
				elif item[0] == "meters":
					Meters = item[1].replace(" ","").split(",")
					EventHandler.AddVirtualMeter(Name,Meters)

	
	EventHandler..PollAndSendEvents(2)
	
	
