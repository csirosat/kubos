#!/bin/bash
set -e

echo "Setup Environment"
source env.sh

#echo "Cleaning build"
#rm -rf ~/.xargo
#cargo clean

APPS="
file-service
scheduler-service
shell-service
telemetry-service
"

for app in $APPS
do
  PKG_CONFIG_ALLOW_CROSS=1 RUST_TARGET_PATH=`pwd` \
	  xargo build --target thumbv7m-unknown-linux-uclibc -p $app --release
done

echo "$APPS" | rsync -av --files-from=- target/thumbv7m-unknown-linux-uclibc/release/ update/usr/local/sbin/

