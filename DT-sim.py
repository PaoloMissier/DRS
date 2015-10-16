#!/usr/bin/env python
from d1_client import d1baseclient
from d1_client.objectlistiterator import ObjectListIterator
from prov.model import ProvDocument
from prov.dot import prov_to_dot
from collections import deque
from datetime import date
import random
import pdb
import os
import matplotlib.pyplot as plt

#configurables
dataOperatorsCount = 2  # 10 different dataOperators
coreRepoSize = 5  # initial repo size (number of ROs)
maxReuseCount = 10  # number of reuse / derivation events
maxCreditUpdateEvents  = 10  # number of random ext credit update events 
randomPickProb = 0.9  # RO will be picked at random for download with prob p, and
                      # it will be extracted from top of queue with prob (1-p)

# constants
DTns = "dt:"

# switches	
createProvletGraphs = True  # produce individual provlets as png graphs?
createDTGraphs      = False  # produce individual DTs as png graphs?
shuffleReuseCreditEvents = False  # do we randomly mix reuse and credit events?

repoSim = {}  # dictionary simulates data repository
ROIdQueue = deque()   # FIFO queue with reinsertion used to ensure balanced reuse across ROs
dataOperators = {}  # workers who upload, download, and derive/reuse


#
# dictionaries used to represent relationships in compound provenance graph
#
#attr    = {}  # attr(entity) -> agent  # attribution
#assoc   = {}   # assoc(activity) -> entity  # association 


#=========================
# RO class
#=========================
class RO:
	def __init__(self, id = None, value=None):
		self.provlet          = None
		self.upstream         = []   # the RO am I derived from / generatedFrom
		self.downstream       = []     # the (RO, act) pairs derived from me
		self.currentExternalCredit  = aCreditManager.initExtCredit(self)
		self.currentTotalCredit =  self.currentExternalCredit
		self.reuseCount = 0
				
		if id == None:
			found = False
			while not found:
				id = str(int(random.random() * 10000))
				if not (id in repoSim.keys()):
					found = True
					self.id = id
		else: 
			self.id = id		
			
		if  value == None:
			self.value = "simVal_"+ str(id)
		else:
			self.value = value

	def pp(self):
		print "RO({id},{value}) with provlet: \n{provlet}".format(id=self.id, value=self.value,provlet=self.provlet.get_provn())


#=========================
# activity class
#=========================
class activity:
	def __init__(self, id = None, type=None):

		if type == None:
			self.type = TypeManager.T0
		else:
			self.type = type	
								
		if id == None:
			self.id = str(int(random.random() * 10000))
		else: 
			self.id = id

        def getType(self):
                return self.type        




#=========================
# TypeManager class manages the types of activities, expressed as vectors (nested lists) of credit transfer parameters
#=========================
class TypeManager:

        typesCatalogue = {}

        ALPHA = 0        # \alpha credit transfer parameter for through activity
        BETA  = 1        # \beta credit transfer parameter for through activity
        GAMMA = 2        # \gamma credit transfer parameter for through activity
        ALPHA_DER = 0.5  # \alpha credit transfer parameter for no-activity derivation

        # example activity types
        T0 = 0
        T1 = 1
        T2 = 2

        # format:  [alpha, beta, gamma]  (no separate beta / gamma for individual inputs/outputs)
        typesCatalogue[0] = [0.5, 1, 0.5]
        typesCatalogue[1] = [0.8, 0.5, 1]

        activityTypes = { 'P1':T0, 'P2':T1 }
        
        def getAlphaDer(self):
                return self.ALPHA_DER

        # param types: ALPHA, BETA, GAMMA
        def get(self, paramType, actType):
                try:
                        return self.typesCatalogue[actType][paramType]
                except ValueError:
                        print "TypeManager Error: unknown paramType {ptype}".format(ptype=paramType)
                        return 0
                
                
#=========================
# dataOperator class
#=========================
class dataOperator:

	def __init__(self, id):
		self.id = id


        # assume aROList is a singleton -- cannot derive from multiple entities!
	def deriveFromRO(self, aROList, derivedROCount=1, derivedIDs=None):
                
                derivedList = []
                for i in range(derivedROCount):
                        if derivedIDs is None:
		                suffix = "_der_"+repr(i)+"_"+repr(aROList[0].reuseCount+1)
                                id = aROList[0].id+suffix
                                value = aROList[0].value+suffix
		                aRO1 = RO(id, value)
                        else:
                                suffix = "_der_"+repr(i)+"_"+repr(aROList[0].reuseCount+1)
                                aRO1 = RO(derivedIDs[i], aROList[0].value+suffix)

                        # upload derived ROs
                        self.uploadRO(aRO1)

                        derivedList.append(aRO1)

                        aRO1.provlet = aDTManager.deriveDependency(self, aROList[0], derivedList)

                for ro in aROList:
                        aEventReporter.addEvent("{dlist} derived from {ro0}".format(dlist=derivedList, ro0=ro.id), repoSim)

		return derivedList  

        
	# generates <derivedROCount> new ROs from aROList through a new activity
	def useThenGenerate(self, aROList,  derivedROCount=1, activityID=None, derivedIDs=None):                

#                pdb.set_trace()

                # create new activity
                if activityID is None:
		        aID = DTns+'act_'+str(int(random.random() * 10000))
                else:
                        aID = activityID
                throughActivity = activity(aID)

                if aID in aTypeManager.activityTypes.keys():
                        throughActivity.type = aTypeManager.activityTypes[aID]   # set that type -- overrides constructor default
                        
                derivedList = []
                for i in range(derivedROCount):                        
                        if derivedIDs is None:
		                suffix = "_ug_"+repr(aROList[0].reuseCount+1)  #FIXME -- create an ID for new RO which includes all the used ones
                                id = aROList[0].id+suffix
                                value = aROList[0].value+suffix
		                aRO1 = RO(id, value)
                        else:
		                suffix = "_ug_"+repr(aROList[0].reuseCount+1)
                                aRO1 = RO(derivedIDs[i], aROList[0].value+suffix)

                        # upload derived ROs
                        self.uploadRO(aRO1)

                        derivedList.append(aRO1)

		aRO1.provlet =  aDTManager.useGenDependency(self, aROList, derivedList, throughActivity)

                # send event to reporter
                usedROIDList = map (lambda x: x.id, aROList)
                for derRO in derivedList:
                        aEventReporter.addEvent("{ro1} genBy {a} used {rolist}".format(ro1=derRO.id, rolist=usedROIDList, a=aID), repoSim)
                
		return derivedList 
	
	
	def generateAndUploadRO(self, ROId=None):
		aRO = RO(id=ROId)
		print "new RO ID: {id}".format(id=aRO.id)
		
		# create provlet with attribution
		aRO.provlet =  aDTManager.generateProvlet(self, aRO)

                self.uploadRO(aRO)

                aEventReporter.addEvent("{ro0} created".format(ro0=aRO.id), repoSim)

		return aRO
	

	# returns a RO. If target ROId not given, then either pick one at random (with prob randomPickProb) or extract from a FIFO queue to ensure regularity of reuse
	def downloadRO(self, aROId = None): 

                if aROId is not None:
                        return repoSim[aROId]
                        
		if random.random > randomPickProb:	
			aROId = ROIdQueue.popleft()
# 			print "removing {id} from queue\n".format(id=ROId)

			ROIdQueue.append(aROId)
		else:
			aROId = random.choice(repoSim.keys())

		aEventReporter.addEvent("download {ro0}".format(ro0=aROId), repoSim)

                return repoSim[aROId]

							
	# simulate uploading by simply recording this ID into a dictionary
	def uploadRO(self, aRO):
		repoSim[aRO.id] = aRO
		ROIdQueue.append(aRO.id)


#=========================
# DT manager class
#=========================

## the DT manager maintains a composite graph, 
## which gets updated with every new incoming provlet

# global prov document
pGlobal = ProvDocument()  # empty provenance document
pGlobal.add_namespace('dt', 'http://cs.ncl.ac.uk/dtsim/')


class DTManager:

        PATH = 'graphs'
        SEP  = '/'

	def notify(self, provDoc):
		print "DT manager adding provlet\n{d}\n".format(d=provDoc.get_provn())

	#=========================
	## generate provlets on demand
	#=========================

	# aRO attributed to aDO
	def generateProvlet(self, aDO, aRO):
		# create provlet
		d1 = ProvDocument()  # d1 is now an empty provenance document
		d1.add_namespace('dt', 'http://cs.ncl.ac.uk/dtsim/')

		e1 = d1.entity(DTns+aRO.id)
		ag1 = d1.agent(DTns+str(aDO.id))
		d1.wasAttributedTo(e1, ag1)

		# update global graph
		e1 = pGlobal.entity(DTns+aRO.id)
		ag1 = pGlobal.agent(DTns+str(aDO.id))
		pGlobal.wasAttributedTo(e1, ag1)

# 		self.notify(d1)	
		return d1


	# aRO1 wasDerivedFrom aRO by aDO
	def deriveDependency(self, aDO, aRO, derivedList):

		d1 = ProvDocument()  # d1 is now an empty provenance document
		d1.add_namespace('dt', 'http://cs.ncl.ac.uk/dtsim/')
		e1 = d1.entity(DTns+aRO.id)   #deriving
		ag1 = d1.agent(DTns+str(aDO.id))
                for der in derivedList:
		        # create provlet
		        e2 = d1.entity(DTns+der.id)   #derived
		        d1.wasAttributedTo(e2, ag1)
		        d1.wasDerivedFrom(e2,e1)
	
		        # update upstream pointer
		        der.upstream = [(aRO, None)]   # aRO is upstream from aRO with no activity
		
		        # update downstream 
		        aRO.downstream.append((der, None))  # aR1 is downstream from aR1 with no activity
		
		# update global graph
		e1 = pGlobal.entity(DTns+aRO.id)   #deriving
		ag1 = pGlobal.agent(DTns+str(aDO.id))
		pGlobal.wasAttributedTo(e2, ag1)
                for der in derivedList:
		        e2 = pGlobal.entity(DTns+der.id)   #derived
		        pGlobal.wasDerivedFrom(e2,e1)
		
		# trigger credit recomputation 
                for der in derivedList:
		        # aRO needs its credit updated with aRO1.credit		
		        aCreditManager.addDerivationCredit(aRO, der.currentTotalCredit)
		
# 		self.notify(d1)	
		return d1
	
	
	def useGenDependency(self, aDO, usedList, genList, throughActivity):

	        aID = throughActivity.id
                
		# create provlet
		d1 = ProvDocument()  # d1 is now an empty provenance document
		d1.add_namespace('dt', 'http://cs.ncl.ac.uk/dtsim/')

                usedEntities = []
                for aRO in usedList:
                        usedEntities.append(d1.entity(DTns+aRO.id))

                genEntities = []
                for aRO1 in genList:
                        genEntities.append(d1.entity(DTns+aRO1.id))
                        
		a  = d1.activity(DTns+aID)
		ag1 = d1.agent(DTns+str(aDO.id))

		d1.wasAssociatedWith(a, ag1)
                for ue in usedEntities:
		        d1.used(a, ue)

                for gene in genEntities:
                        d1.wasAttributedTo(gene, ag1)
		        d1.wasGeneratedBy(gene,a)

                # associate this provlet to each generated RO
                for aRO1 in genList:
                        aRO1.provlet = d1

                        
		print "event {n}: DO {do}: {ro1} <- wgby <- {act} <- used {ro}".format(n=currentReuseCount, do=aDO.id, ro1=aRO1.id, act=aID, ro=aRO.id)

                for genRO in genList:
                        for uRO in usedList:
		                # update upstream pointer
                                genRO.upstream.append(( uRO, throughActivity ))  # dep on aRO through activity aID   FIXME URGENTLY!!!  not designed for M-M

                for uRO in usedList:
                        for genRO in genList:
		                # update downstream 
		                uRO.downstream.append((genRO, throughActivity))  # aR1 is downstream from aR1 through activity aID

		# update global graph
                globalUsedEntities = []
                for aRO in usedList:
                        globalUsedEntities.append(pGlobal.entity(DTns+aRO.id))

                globalGenEntities = []
                for aR1 in genList:
                        globalGenEntities.append(pGlobal.entity(DTns+aR1.id))

		a  = pGlobal.activity(DTns+aID)
		ag1 = pGlobal.agent(DTns+str(aDO.id))

		pGlobal.wasAssociatedWith(a, ag1)
                for ue in globalUsedEntities:
		        pGlobal.used(a, ue)

                for gene in globalGenEntities:
                        pGlobal.wasAttributedTo(gene, ag1)
		        pGlobal.wasGeneratedBy(gene,a)

		# trigger credit recomputation 
		# each used RO needs its credit updated with aRO1.credit for each generated aRO1 through activity aID
		aCreditManager.addGenerationCredit(usedList, genList, throughActivity)  

# 		self.notify(d1)	
		return d1
		

	# returns the connected fragment of global graph rooted at RO 
	def computeDT(self,aRO):
		aDT = ProvDocument()  
		aDT.add_namespace('dt', 'http://cs.ncl.ac.uk/dtsim/')
		self.recComputeDT(aRO, aDT)	
# 		print "final DT: {dt}".format(dt=aDT.get_provn())
		return aDT
		
		
	def recComputeDT(self,aRO, aDT):		
		eRO = aDT.entity(DTns+aRO.id)
# 		print "\n**** \ninterim DT after adding entity for {ro}: {dt}".format(ro=aRO.id,dt=aDT.get_provn())
		for (aRO1, act) in aRO.downstream:
			eRO1 = aDT.entity(DTns+aRO1.id)
			
			print "downstream from {ro}:  {ro1} with act {a}".format(ro=aRO.id, ro1=aRO1.id, a=act)

			if act is not None:
				aY = aDT.activity(DTns+act)
				aDT.used(aY, eRO)
				aDT.wasGeneratedBy(eRO1, aY)
			else:			
				aDT.wasDerivedFrom(eRO1, eRO)
			self.recComputeDT(aRO1, aDT)	

		return aDT

	
	def  reportROTrajectories(self):
		for RO in repoSim.values():
			DT = self.computeDT(RO)
			dot = prov_to_dot(DT)
			dot.write_png("DT_"+RO.id+'.png')

	
 	def reportGlobalGraph(self):
		dot = prov_to_dot(pGlobal)
                if os.path.exists(self.PATH):
		        dot.write_png(self.PATH + self.SEP + 'pGlobal.png')
                        f = open(self.PATH + self.SEP+'pGlobal.provn', 'w')
                        f.write(pGlobal.get_provn())
                else:
                        print "path not found: {p}".format(p=self.PATH)

                               
	def reportReuseStats(self):
		print "\n****  RO reuse count stats:  ****\n"

		total = 0
		for RO in repoSim.values():
			print "{id}\t{n}".format(id=RO.id, n=RO.reuseCount)
			total = total + RO.reuseCount
		print "total reuse events:  {n}\n".format(n=total)


	def reportROProvlets(self):
                if os.path.exists(self.PATH):
		        for RO in repoSim.values():
 		#	        print "RO: {ro}:\n{provlet}\n*****".format(ro=RO.id, provlet=RO.provlet)

			        dot = prov_to_dot(RO.provlet)
			        dot.write_png(self.PATH+self.SEP+RO.id+'.png')
                                f = open(self.PATH + self.SEP+RO.id+'.provn', 'w')
                                f.write(RO.provlet.get_provn())
                else:
                        print "path not found: {p}".format(p=self.PATH)
	
		
		

#=========================
# credit manager class
#=========================

class CreditManager:

	def initExtCredit(self, aRO):   # default initial external credit
		return 1   
		
        # the current ext credit of aRO is replaced by newExtCredit
        # the new total credit for aRO is then propagated upstream
	def updateExtCredit(self, aRO, newExtCredit):
	
		oldExtCredit = aRO.currentExternalCredit
		oldTotCredit = aRO.currentTotalCredit

		aRO.currentExternalCredit = newExtCredit		
		aRO.currentTotalCredit    = self.updateTotCredit(aRO, oldExtCredit, newExtCredit)
		self.propagateCreditUpstreamFrom(aRO, oldTotCredit, aRO.currentTotalCredit)
		
		
	def updateTotCredit(self, aRO, oldExtCredit, newExtCredit):  # simplest possible model!
		return aRO.currentTotalCredit - oldExtCredit + newExtCredit
			

	def propagateCreditUpstreamFrom(self, aRO, old, new):

#                pdb.set_trace()

                for i in range(len(aRO.upstream)):
                        # aRO.upstream[i]: (RO, P)
                        # upstreamRO = aRO.upstream[i]
	 		#print "\tpropagating credit update from {ro} to {ro1}".format(ro=aRO.id, ro1=upstreamRO.id)
			self.updateCredit(aRO.upstream[i], old, new)
		


                        
	# bottom up credit recomputation
	# called when a direct dependent's credit changes
	# oldCredit and newCredit are those of the dependent
	# activity is None if the dependency is a derivation
	def updateCredit(self, (aRO, throughActivity), oldCredit, newCredit):

                if throughActivity is not None:
                        aID = throughActivity.id
                else:
                        aID = "No Activity"
                        
		print "updateCredit called on ({ro},{act}) with old = {old}, new = {new}".format(ro=aRO.id, act=aID, old=oldCredit, new=newCredit)

                thisOldCredit = aRO.currentTotalCredit
		if throughActivity is None:
			aRO.currentTotalCredit = aRO.currentTotalCredit - oldCredit + newCredit  # initial naive credit model
		else:
                        alpha = aTypeManager.get(TypeManager.ALPHA, throughActivity.getType())
			aRO.currentTotalCredit = aRO.currentTotalCredit +  (newCredit - oldCredit) * alpha

		self.propagateCreditUpstreamFrom(aRO, thisOldCredit, aRO.currentTotalCredit) # keep propagating recursively


        ####                
        # assign new credit newCredit to aRO
        ####
        def addDerivationCredit(self, aRO, newCredit):
	
		thisOldCredit = aRO.currentTotalCredit
                weightedNewCredit = aTypeManager.getAlphaDer() * newCredit
                aRO.currentTotalCredit = aRO.currentExternalCredit + weightedNewCredit 

		print "creditManager: raw new Credit: {raw}. adding der credit {x} to {id}".format(raw=newCredit, x=weightedNewCredit, id=aRO.id)
		print "now {id} has ext={external}, total={total}".format(id=aRO.id, external=aRO.currentExternalCredit, total=aRO.currentTotalCredit)
                
		self.propagateCreditUpstreamFrom(aRO, thisOldCredit, aRO.currentTotalCredit) # keep propagating recursively


        ####
        # propagates the external credit from each element in genList (the new ROs), to each element in the usedList (the previously existing ROs)
        # TODO FIXME  see pattern in the paper
        # because 
        ####
	def addGenerationCredit(self, usedList, genList, throughActivity):

                n = len(usedList)  # use this to normalise credit (see formula in the paper)
                
                alpha = aTypeManager.get(TypeManager.ALPHA, throughActivity.getType())
                beta  = aTypeManager.get(TypeManager.BETA, throughActivity.getType())
                gamma = aTypeManager.get(TypeManager.GAMMA, throughActivity.getType())

                if len(genList) == 1:
                        sumsOfGenCredit = genList[0].currentTotalCredit
                else:
                        sumsOfGenCredit = reduce (lambda x,y: x.currentTotalCredit + y.currentTotalCredit, genList) 

                newCredit = alpha * beta / n * gamma * sumsOfGenCredit
                
                for aRO in usedList:
                        # this is a new usage so it just adds to any existing prior credit -- nothing to be subtracted
                        
		        thisOldCredit = aRO.currentTotalCredit                        
		        aRO.currentTotalCredit = aRO.currentTotalCredit + newCredit

		        print "creditManager adding gen credit {x} to {id} through activity {act}".format(x=newCredit, id=aRO.id, act=throughActivity.id)
		        print "now {id} has ext={external}, total={total}".format(id=aRO.id, external=aRO.currentExternalCredit, total=aRO.currentTotalCredit)

	                self.propagateCreditUpstreamFrom(aRO, thisOldCredit, aRO.currentTotalCredit) # keep propagating recursively, using RO's old credit as baseline



	
	def reportCredits(self):
		print "\n**** current credit values ****\n"
		for aRO in repoSim.values():
			print "{ro}:  {ext}, {total}".format(ro=aRO.id, ext=aRO.currentExternalCredit, total=aRO.currentTotalCredit)
			


#=========================
# Event Reporter
#=========================

event = {}             # event: eventId -> (desc, creditSnapshot)
creditSnapshot = {}    # creditSnapshot: ROId --> totCredit

# example of usage
#  creditSnapshot[RO_1] = 10, creditSnapshot[RO_2] = 20
#  event[1] = ("reuse XYZ",  [RO_1] = 10, creditSnapshot)
#  

class EventReporter:

        def __init__(self):
                self.eventId = 0
                self.allROs = []
                self.DEFAULT_RAW_FILE = 'rawEvents.log'
                self.DEFAULT_REPORT_FILE = 'plottableEvents.log'

                
        def addEvent(self, desc, repoSim):
                creditSnapshot = {}
                for roID in repoSim.keys():   
                        creditSnapshot[roID] = repoSim[roID].currentTotalCredit
                        if roID not in self.allROs:
                                self.allROs.append(roID)
                        
                self.eventId = self.eventId + 1
                event[self.eventId] = ("["+desc+"]", creditSnapshot)
                print "added event: \n{ev}".format(ev=event[self.eventId])


        def printEvents(self, f = None):

                if f is None:
                        outfile = open(self.DEFAULT_RAW_FILE,'w')
                else:
                        outfile = open(f, 'w')
                        
                
                header = "event number \tdescription\t"
                for ROId in repoSim.keys():
                        header = header + ROId +"\t"
                outfile.write(header+'\n')

                for evKey in event.keys():
                        (desc, creditDict) = event[evKey]
                        outfile.write("{id}\t{d}\t{credits}\n".format(id=evKey, d=desc, credits=map(lambda x: x, creditDict.values())))
                

        def plotEvents(self, dump = True, f = None):

                print "all ROids for plotting: {roids}".format(roids=self.allROs)

                if dump is True and f is None:
                        outfile = open(self.DEFAULT_REPORT_FILE,'w')
                elif dump is True:
                        outfile = open(f, 'w')
                        
                ## create X, Y datapoints
                
                X = []  # event descriptions on x axis
                XDesc = []
                Y = {}  # dict to hold Y values for each RO
                events2Desc = {}
                maxY = 0
                for id in event.keys():   # events become columns
                        desc   = event[id][0]
                        values = event[id][1] 
                        X.append(id)
                        XDesc.append(desc)
                        events2Desc[id] = desc
                        
                        for ROId in self.allROs:
                                if ROId not in Y.keys():
                                        Y[ROId] = []                                        
                                if ROId in values.keys():
                                        Y[ROId].append(values[ROId])
                                        if maxY < values[ROId]:
                                                maxY = values[ROId]
                                else:
                                        Y[ROId].append(0)

                # plot them up
                plt.figure(1)
                plt.subplot(211)
                for ROId in Y.keys():
                        plt.plot(X, Y[ROId], '--o', label = ROId)

                #plt.title("Total credit changes to ROs following reuse and external credit adjustment events")
                plt.xlim(1, len(X)+1)
                plt.xticks(X, XDesc, rotation=90)
                plt.ylim(0, maxY+1)
                # Place a legend to the right of this smaller figure.
                plt.legend(bbox_to_anchor=(1.05, 1), loc=2, prop={'size':11})

                #plt.xlabel("data reuse and external credit update events")
                plt.ylabel("total credit")
                plt.grid(True)
                plt.show()

                # optionally write to file
                if dump is True:
                        print "writing event id --> event desc to {f}\n".format(f=outfile.name)
                        for id in events2Desc.keys():
                                outfile.write("{ev}\t{desc}\n".format(ev=id, desc=events2Desc[id]))
                                              

                
                                        

                
#=========================
#
# simulator
#
#=========================

currentReuseCount = 0
currentCreditUpdateCount = 0
DEFAULT_DERIVED_COUNT = 3

def randomRODownload():
	# random DOs download, derive or use/generate, and upload
	# pick a random DO
	aDOId = random.choice(dataOperators.keys())
	DO = dataOperators[aDOId]
	return DO.downloadRO()	# pick a random RO in the repo  (possibly already reused)


# doReuse: True -> useThenGenerate, False -> DerivedFrom, None: random
def simReuseEvent(reusingDO = None, reusedROs = None, activityID=None, useGen = None, derCount = None, derIDs=None):

	print "\n**** simreuse() {n}".format(n=currentReuseCount)
	
        if reusingDO is None:
	        # pick a random DO
	        aDOId = random.choice(dataOperators.keys())
	        DO = dataOperators[aDOId]
        else:
                DO = reusingDO
                
	if reusedROs is None:
		aROList = [randomRODownload()]
	else:
		aROList = reusedROs

	if derCount is None:
                derivedCount = DEFAULT_DERIVED_COUNT
                
	# worker operates on RO, derives or use/gen, and uploads the resulting ROS
        # force derivation
        # aROList (the deriving ROs) should really be a singleton!!  cannot derive from multiple entities
	if useGen is False:
		derivedROList = DO.deriveFromRO(aROList, derivedROCount=derCount, derivedIDs=derIDs)

        # force use/gen
	elif useGen is True:     
		derivedROList = DO.useThenGenerate(aROList, derivedROCount=derCount, activityID=activityID, derivedIDs=derIDs)

        # who knows!
	elif random.random() < 0.3:  # if doReuse not specified then decide: derive or use/gen?
		derivedROList = DO.deriveFromRO(aROList)
	else:
		derivedROList = DO.useThenGenerate(aROList)

        # FIXME should be +derivedCount times but this screws up the naming sequence for new ROs!
        for ro in aROList:
	        ro.reuseCount = ro.reuseCount + 1   # aRO has now been reused 1 more time.
        	print "updated reuse count for {n} is now {x}".format(n=ro.id, x=ro.reuseCount)

	# upload new derived objects
#	for derRO in derivedROList:	
#		DO.uploadRO(derRO)
		
	return derivedROList



def simCreditUpdateEvent(targetRO = None, newExtCredit = None):
	print "\n**** simUpdateCredit() {n}".format(n=currentCreditUpdateCount)

	if targetRO is None:
		# pick one of the ROs from the repo		
		aRO = randomRODownload()
	else:
		aRO = targetRO
		
	if newExtCredit is None:
		# randomly set new ext credit
		newCredit = random.choice(range(1,10))
	else:
		newCredit = newExtCredit 		
		
	# then call updateExtCredit
 	print "updating external credit for RO {ro} to {new}".format(ro=aRO.id,new=newCredit)
	aCreditManager.updateExtCredit(aRO, newCredit)

        aEventReporter.addEvent("New ext credit for {ro}={cr}".format(ro=aRO.id, cr=newCredit), repoSim)



def shuffleEvents(currentReuseCount = 0, currentCreditUpdateCount = 0):
	print "\n a mix of {n} RO reuse and {m} external credit update events starting: \n".format(n=maxReuseCount, m=maxCreditUpdateEvents)

	random.seed()
	while currentCreditUpdateCount < maxCreditUpdateEvents or currentReuseCount < maxReuseCount:
		if random.random() < 0.5:   # <0.5 reuse event, => 0.5 credit update event
			if currentReuseCount < maxReuseCount:
				simReuseEvent()
				currentReuseCount += 1 
			else:
				print "max reuse count {mrc} reached".format(mrc=maxReuseCount)		
		else:
			if currentCreditUpdateCount < maxCreditUpdateEvents:
				simCreditUpdateEvent()
				currentCreditUpdateCount += 1
			else:
				print "max credit update events count {mcue} reached".format(mcue=maxCreditUpdateEvents)


def sequentialEvents(currentReuseCount = 0, currentCreditUpdateCount = 0):

	print "\n {n} RO reuse events starting: \n".format(n=maxReuseCount)
	while currentReuseCount < maxReuseCount:
		simReuseEvent()
		currentReuseCount += 1 

	print "\n  {m} external credit update events starting: \n".format(m=maxCreditUpdateEvents)
	while currentCreditUpdateCount < maxCreditUpdateEvents:
		simCreditUpdateEvent()
		currentCreditUpdateCount += 1

                
#
# coreRepoSize ROs created
# then a sequence of reuse/credit events either random or sequential (first create tolopolgy, then adjust credit)
#
def randomSim():
	# generate seed repo content
	for n in range(0,coreRepoSize):
		aDOId = random.choice(dataOperators.keys()) 	# pick a dataOperator at random
		DO = dataOperators[aDOId]
		aRO = DO.generateAndUploadRO()
	
	
	print "\n now {n} ROs in repo\n".format(n=repr(n+1))
	print " --- "
	# print "\ndeque after seeding repo:\n"
	# for i in ROIdQueue:
	# 	print repr(i)


	if shuffleReuseCreditEvents:
		shuffleEvents()
	else:
		sequentialEvents()


####
#  simplest test case -- one RO, one reuse event, one credit update event
####
def sampleScript1():	

	print "****\nrunning script 1\n****"
	
	aDOId = random.choice(dataOperators.keys()) 	# pick a dataOperator at random
	DO = dataOperators[aDOId]
	
	# one seed RO
	aRO = DO.generateAndUploadRO()
	print "script1: RO {ro} generated and uploaded".format(ro=aRO.id)	

        # derivation event
	print "script1: one usage/gen event. using RO {ro}".format(ro=aRO.id)	
	derivedROList = simReuseEvent(reusedROs=[aRO], useGen = False, derCount=2)   # derivedFrom

        #        print "derivedROList:"
        #        for ro in derivedROList:
        #                print "derived RO: {r}".format(r=ro.id)
        
        # a sample credit update event for each of the derived ROs
        newCredit = 5
        for ro in derivedROList:
                print "script1:  credit update event for derived ro {x} with new credit {new}".format(x=ro.id, new=newCredit)
                simCreditUpdateEvent(ro, newCredit)  # new credit = 5

        # one usage/gen event
        generatedROList = simReuseEvent(reusedRO=[aRO], useGen = True, derCount=2)   # use/gen

        # credit update event for each of the generated ROs
        newCredit = 3
        for ro in derivedROList:
	        print "script1:  credit update event for generated ro {x} with new credit {new}".format(x=ro.id, new=newCredit)
	        simCreditUpdateEvent(ro, newCredit)  # new credit = 5




####
#  the DCC 16 paper submssion scenario
####
def paperScript():	

	print "****\nrunning papeScript\n****"
	
	aDOId = random.choice(dataOperators.keys()) 	# pick a dataOperator at random
	DO = dataOperators[aDOId]
	
	# one seed: RO1
	aRO1 = DO.generateAndUploadRO("RO1")
	print "script1: RO {ro} generated and uploaded".format(ro=aRO1.id)	

        # P1 used RO1 and generated RO2, RO3
 	print "paperScript: one usage/gen event. using RO {ro}, generating RO2, RO3".format(ro=aRO1.id)	
        derivedROList = simReuseEvent(reusedROs=[aRO1], useGen = True, activityID="P1", derCount=2, derIDs = ["RO2", "RO3"])   # useGen

        # P2 used RO1, RO3  and generated RO4
 	print "paperScript: one usage/gen event. using RO1, RO3, generating RO4"	
        aRO3 = DO.downloadRO("RO3")
        derivedROList = simReuseEvent(reusedROs=[aRO1, aRO3], useGen = True, activityID="P2", derCount=1, derIDs = ["RO4"])   # useGen
        
        # credit update event for RO4
        newCredit = 3
        aRO4 = DO.downloadRO("RO4")
        print "paperScript:  credit update event for generated ro {x} with new credit {new}".format(x=aRO4.id, new=newCredit)
        simCreditUpdateEvent(aRO4, newCredit)  

        # <X> used RO2, RO3 and generated RO5
 	print "paperScript: one usage/gen event. using RO2, RO3, generating RO5"	
        aRO2 = DO.downloadRO("RO2")
        derivedROList = simReuseEvent(reusedROs=[aRO2, aRO3], useGen = True, derCount=1, derIDs = ["RO5"])   # useGen

        ##
        # stable topology as per paper example
        # credit updates only below
        ##
        
        # a sample credit update event for RO5
        newCredit = 5
        aRO5 = DO.downloadRO("RO5")
        print "paperScript:  credit update event for ro {x} with new credit {new}".format(x=aRO5.id, new=newCredit)
        simCreditUpdateEvent(aRO5, newCredit)  



        
#=========================
# main body
#=========================

print "\n#### BEGIN ####\n"

# create singletons for global functions

aDTManager     = DTManager()
aCreditManager = CreditManager()
aTypeManager   = TypeManager()
aEventReporter = EventReporter()

# create dataOperators
for i in range(0, dataOperatorsCount):
	dataOperators[i] = dataOperator('DO-'+str(i))	


#randomSim()
#sampleScript1()
paperScript()

print "\n**** END OF SIMULATION ****\n"

        
aDTManager.reportGlobalGraph()
aDTManager.reportReuseStats()
aCreditManager.reportCredits()
if createProvletGraphs:
	aDTManager.reportROProvlets()
	
if createDTGraphs:
	aDTManager.reportROTrajectories()

print "\n*** Events timeline: ****\n"
aEventReporter.printEvents()	
aEventReporter.plotEvents()	
