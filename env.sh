#!/bin/bash

ROOT=`git rev-parse --show-superproject-working-tree`
TOOLCHAINS=$ROOT/toolchains
THIRDPARTY=$ROOT/third-party
DEPS=$THIRDPARTY/uclinux-port/dependencies

export  CC=$TOOLCHAINS/buildroot/host/bin/arm-buildroot-uclinux-uclibcgnueabi-gcc
export CXX=$TOOLCHAINS/buildroot/host/bin/arm-buildroot-uclinux-uclibcgnueabi-g++
export XARGO_RUST_SRC=$DEPS/rust/src

