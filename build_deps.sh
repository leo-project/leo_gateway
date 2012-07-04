#!/bin/bash

BASEDIR="$PWD"

if [ ! -d cherly ]; then
	git clone https://github.com/mocchira/cherly.git
fi
(cd cherly && ./configure && make)
if [ $? -ne 0 ]; then
	echo "you probably need to do 'cd cherly; sudo make install' manually."
fi
