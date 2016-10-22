#!/Users/joeblue/anaconda/bin/python
import json
import numpy as np
import random

### settings ###
customers=100000 ### number of customers 
rate=0.25        ### rate of "good" accounts
numLoans=12      ### number of loan products

### the following attempt to add a signal into the sample ###
p = [120,110,110,85,84,78,54,51,49,12,9,8] ### distribution of loans (default)
goodP = [60,60,70,40,23,45,33,11,20,2,25,2] ### distribution of loans (good)
probs = [x / float(sum(p)) for x in p]
goodProbs = [x / float(sum(goodP)) for x in goodP]

### create sample of good accounts ###
goods=random.sample(range(0,customers),int(customers*rate)) 

### create sample of number of loans ###
loans = np.random.poisson(4, customers)

############################################
### generate samples and write to stdout ###
############################################
id=0
for n in loans:
  if id in goods:
     products = np.random.choice(numLoans, min(n+1,11), replace=False, p=goodProbs)
     for a in products:
       print '{acct:"%s",loanType:"%s",good:"%d"}' % (id,a,1)
  else:
     products = np.random.choice(numLoans, min(n+1,11), replace=False, p=probs)
     for a in products:
       print '{acct:"%s",loanType:"%s",good:"%d"}' % (id,a,0)
  id+=1
