leo_gateway
============

[![Build Status](https://secure.travis-ci.org/leo-project/leo_gateway.png?branch=master)](http://travis-ci.org/leo-project/leo_gateway)

Overview
--------

* "leo_gateway" is one of the core component of [LeoFS](https://github.com/leo-project/leofs). Main roles are described below.
  * LeoFS's Gateway use [Cowboy](https://github.com/extend/cowboy) as Erlang's HTTP server.
  * Able to speak [Amazon S3 compatible REST API](http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/Welcome.html?r=5754).
  * Implemented a subset of Caching in HTTP(RFC2616).
*  Detail document is [here](http://www.leofs.org/docs/).
* "leo_gateway" uses [rebar](https://github.com/basho/rebar) build system. Makefile so that simply running "make" at the top level should work.
* "leo_gateway" requires Erlang R15B03-1 or later.

