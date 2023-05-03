#!/bin/bash
# OSS-fuzz build script

compile_native_go_fuzzer $SRC/bloom/fuzz FuzzBloom fuzz_bloom
