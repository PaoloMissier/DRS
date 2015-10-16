# DRS: Data Reuse Simulator
=========

This program simulates data reuse.
It simulates Research Objects in a repository, and Agents, known as Data Operators (DO) that upload/download/reuse those ROs

It generates two types of events:

- a new instance of reuse of an existing RO by a DO.  This can be:
    - a derivation (no explicit activity)
	- a use/generation event through an explicit activity 
- updates to the External Credit associated with an RO

For each event, the simulator:

-  generates a corresponding provlet and updates the global provenance graph. Note that in reality this would be obtained as composition of provlets. In the simulation, composition is trivial because all RO IDs match by default.
- propagates the credit from the derived RO to the upstream deriving ROs

The sequence of events can be simulated in two main ways:

- script the exact sequence, down to the number of inputs/outputs for each simulated activity. 
  This is achieved by calling the appropriate methods. Examples are in functions script1() and paperScript()
  
- set simulation parameters as random variables, and let the simulator do the rest.
  Currently you can set:
    - how many times an RO is reused
    - whether the choice of RO is random or based on a FIFO queue (dequeue)
    - how many ROs are in the repository
    - probability of use/gen vs derivation
    - probability that the external credit to an RO changes

Much more to come, in time...



