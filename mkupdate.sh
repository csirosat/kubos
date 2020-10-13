#!/bin/sh

tar --owner=root:0 --group=root:0 -czvf update.tgz -C update usr/ \
	&& rsync -av update.tgz /srv/tftp/

