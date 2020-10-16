#!/bin/bash

set -e

pushd update
find usr/ -type f | xargs md5sum > checksums.md5
popd
tar --owner=root:0 --group=root:0 -czvf update.tgz -C update checksums.md5 usr/
rsync -av update.tgz /srv/tftp/

