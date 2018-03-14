This tool publishes arbitrary json messages to kafka for testing purposes.  Message can be either strings or json.

Commandline Arguments
=====================
<pre>
Option                 Description
------                 -----------
-d, --delay <Integer>  seconds delay between messages (default: 0)
-h, --help             show help
-m, --messages         file with messages
-s, --servers          kafka servers
-t, --topic            kafka topic
</pre>

See example-input-file.json for synatx of the input file.

Example Use
===========
<pre>
cd kafka-publisher
java -jar build/libs/hbc-kafka-publisher-all.jar --servers=usnc1a-dkfka02.qiotec.internal:9091,usnc1a-dkfka03.qiotec.internal:9091 --topic=sometopic --delay=1 --messages=example-input-file.json
</pre>

Unix Build
==========
<pre>
./gradlew shadowJar
</pre>

Windows Build:
==============
<pre>
gradlew.bat shadowJar
</pre>

ZZZ