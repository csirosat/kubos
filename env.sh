#!/bin/bash

ROOT=`git rev-parse --show-superproject-working-tree`
TOOLCHAINS=$ROOT/toolchains
THIRDPARTY=$ROOT/third-party

export     CC=$TOOLCHAINS/buildroot/host/bin/arm-buildroot-uclinux-uclibcgnueabi-gcc
export    CXX=$TOOLCHAINS/buildroot/host/bin/arm-buildroot-uclinux-uclibcgnueabi-g++
export FLTHDR=$TOOLCHAINS/buildroot/host/bin/arm-buildroot-uclinux-uclibcgnueabi-flthdr
export XARGO_RUST_SRC=$THIRDPARTY/rust/src

echo "Environment Configured Successfully"

