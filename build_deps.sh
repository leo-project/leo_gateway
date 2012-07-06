#!/bin/sh

BASEDIR="$PWD"

if [ ! -d cherly ]; then
	git clone https://github.com/leo-project/cherly.git 
fi

# for some systems including NetBSD, GNU make is commonly installed as "gmake".
MAKE=make
if type gmake > /dev/null; then
	MAKE=gmake
fi

(cd cherly && ./configure && ${MAKE})
if [ $? -ne 0 ]; then
	echo "you probably need to do 'cd cherly; sudo make install' manually."
fi
