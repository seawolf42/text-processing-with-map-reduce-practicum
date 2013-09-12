text-processing-with-map-reduce-practicum
=========================================

Implementations of algorithms from the paper "Data-Intensive Text
Processing with MapReduce"

Developers reading the paper "Data-Intensive Text Processing with MapReduce" by Lin
and Dyer (2010) are presented with several algorithms in pseudo-code for implementing
several common MapReduce patterns for text processing and mathematical analysis. This
repository is intended to provide actual working implementations of these algorithms
built with Hadoop's "new" (as of this writing) API.

This code is written for readability first, not for efficiency. For instance, the
various Writable types used to pass parameters between phases are instantiated
individually as needed rather than being re-used. This results in a large overhead
of object creation and destruction but makes the code much easier to follow.

Assuming you are using Maven and have Hadoop installed on localhost (setting up Hadoop
is beyond the scope of this project but can be performed relatively easily on OSX
[http://jeff.jke.net/2012/10/28/installing-hadoop-dev-environment-mac-osx-snow-leopard]
or Linux:
* compile the project:
      $ mvn package
* run the algorithm Figure 4.2:
      $ hadoop jar target/ditp-1.0-SNAPSHOT.jar ch04.Figure_4_2 corpus output
* clean up when you're done:
      $ rm -rf output

This code may be freely used as per the MIT license. Please attribute this repository
when incorporating this into any derivative work.
