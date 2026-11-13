PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=hdfs
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Linux 标准测试入口执行桥接回归，保留公共 Makefile 的 SQL 测试配方。
ifeq ($(shell uname -s),Linux)
.PHONY: hdfs_check_release hdfs_check_debug hdfs_check_reldebug
test_release_internal: hdfs_check_release
hdfs_check_release:
	cmake --build build/release --target hdfs_open_mode_check hdfs_secret_check
test_debug_internal: hdfs_check_debug
hdfs_check_debug:
	cmake --build build/debug --target hdfs_open_mode_check hdfs_secret_check
test_reldebug_internal: hdfs_check_reldebug
hdfs_check_reldebug:
	cmake --build build/reldebug --target hdfs_open_mode_check hdfs_secret_check
endif
