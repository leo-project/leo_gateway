leo_gateway
============

Overview
--------

* "leo_gateway" is one of the core component of [LeoFS](https://github.com/leo-project/leofs). Main roles are described below.
  * HTTP server as the _Gate_ of _LeoFS_ powered by [mochiweb](https://github.com/mochi/mochiweb).
  * Also, We will plan to support [cowboy](https://github.com/essen/cowboy) with Ver-0.9.1.
  * Able to speak [Amazon S3 compatible REST API](http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/Welcome.html?r=5754).
  * Implemented a subset of Caching in HTTP(RFC2616).

*  Detail document is [here](http://www.leofs.org/docs/).

* "leo_gateway" uses the "rebar" build system. Makefile so that simply running "make" at the top level should work.
  * [rebar](https://github.com/basho/rebar)
* "leo_gateway" requires Erlang R14B04 or later.
