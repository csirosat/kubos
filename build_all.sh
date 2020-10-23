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

TARGET_DIR="target/thumbv7m-unknown-linux-uclibc/release/"
DEST_DIR="../../linux-m2s/projects/horus/update/usr/local/sbin/"

echo "$APPS" | rsync -av --files-from=- $TARGET_DIR $DEST_DIR

