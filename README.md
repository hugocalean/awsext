#AWS Connection Extensions

##Overview
This Python project contains extensions to AWS Connection classes (i.e. EC2Connection), primarily to support
synchronous calls and to check for resource existence (i.e. does an SQS Queue exist.  Most AWS calls
to create some resource (i.e. security group, SQS queue, etc.) will return immediately but the resource will
not exist yet - these routines will poll until the resource exists (or doesn't exist if it was deleted) and 
return upon success or timeout.  

##SSH
SSH'ing into remote EC2 instances is supported by the following classes in the awsext.ec2 package - RemoteRunThread, RemoteRunRequest
and RemoteRunResponse.  An instance of RemoteRunThread can be started to SSH Connect to an EC2 instance, transfer files
and run commands.  These classes are helpful for automating deployments across clusters.  

##Spot Prices
awsext.ec2.spotprice contains methods to determine the cheapest spot prices based on a list of regions

##SQS Durable Messages
The SqsMessageDurable durable class encapsulates automatic reconnection with SQS Message Send/Receive.
If a send or receive fails, reconnection/retry will occur transparently to the caller.  This helps with the 
situation where an SQS is being polled on a regular basis - eventually the connection will drop - this code 
encapsulates the reconnection.

##Installation
* Open your IDE - for this example, PyDev 
* Download the awsext project from github into your IDE
* Run setup.py with the parms: sdist --formats-gztar
* Verify build succeeded: Gzip should be created: ./dist/awsext-1.1.tar.gz
* Copy awsext-1.1.tar.gz to target location (i.e. SCP to an EC2 instance)
* Login to target system and CD to target directory
* Execute the following commands:
	* sudo rm -rf awsext-1.1
	* tar -xvzf awsext-1.1.tar.gz
	* cd awsext-1.1
	* sudo python setup.py install
	* cd ..
	* sudo rm -rf awsext-1.1
* Verify successful installation
	* Start Python interactive and enter:
		* import awsext 
		* print awsext.Version
	* Should display "1.1" - if not, then research the error


