#
# SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
# SPDX-License-Identifier: Apache-2.0
#

USER_ID      = $(shell id -u)
HAS_JEMALLOC = $(shell test -f /usr/local/lib/libjemalloc.a && echo "jemalloc")
JEMALLOC_URL = "https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2"


.PHONY: all badger test jemalloc dependency duckdb-smoke duckdb-compare duckdb-compare-extended duckdb-epoch duckdb-profile duckdb-microbench duckdb-lockfree-compare duckdb-ashley duckdb-ashley-readpool-sweep duckdb-ashley-flushbatch-sweep duckdb-report-latest duckdb-full july30-duckdb-gate july30-duckdb-smallbank july30-duckdb-bank-compare july30-duckdb-readheavy july30-duckdb-summary july30-duckdb-full july30-duckdb-overnight-100m july30-duckdb-overnight-40m

badger: jemalloc
	@echo "Compiling Badger binary..."
	@$(MAKE) -C badger badger
	@echo "Badger binary located in badger directory."

test: jemalloc
	@echo "Running Badger tests..."
	@./test.sh

jemalloc:
	@if [ -z "$(HAS_JEMALLOC)" ] ; then \
		mkdir -p /tmp/jemalloc-temp && cd /tmp/jemalloc-temp ; \
		echo "Downloading jemalloc..." ; \
		curl -s -L ${JEMALLOC_URL} -o jemalloc.tar.bz2 ; \
		tar xjf ./jemalloc.tar.bz2 ; \
		cd jemalloc-5.2.1 ; \
		./configure --with-jemalloc-prefix='je_' --with-malloc-conf='background_thread:true,metadata_thp:auto'; \
		make ; \
		if [ "$(USER_ID)" -eq "0" ]; then \
			make install ; \
		else \
			echo "==== Need sudo access to install jemalloc" ; \
			sudo make install ; \
		fi \
	fi

dependency:
	@echo "Installing dependencies..."
	@sudo apt-get update
	@sudo apt-get -y install \
    	ca-certificates \
    	curl \
    	gnupg \
    	lsb-release \
    	build-essential \
    	protobuf-compiler \

duckdb-smoke:
	@bash ./scripts/duckdb_experiments.sh smoke

duckdb-compare:
	@bash ./scripts/duckdb_experiments.sh compare

duckdb-compare-extended:
	@bash ./scripts/duckdb_experiments.sh compare-extended

duckdb-epoch:
	@bash ./scripts/duckdb_experiments.sh epoch

duckdb-profile:
	@bash ./scripts/duckdb_experiments.sh profile

duckdb-microbench:
	@bash ./scripts/duckdb_experiments.sh microbench

duckdb-lockfree-compare:
	@bash ./scripts/duckdb_experiments.sh lockfree-compare

duckdb-ashley:
	@bash ./scripts/duckdb_experiments.sh ashley

duckdb-ashley-readpool-sweep:
	@bash ./scripts/duckdb_experiments.sh ashley-readpool-sweep

duckdb-ashley-flushbatch-sweep:
	@bash ./scripts/duckdb_experiments.sh ashley-flushbatch-sweep

duckdb-report-latest:
	@latest=$$(ls -1dt artifacts/duckdb/* 2>/dev/null | head -n 1); \
	if [ -z "$$latest" ]; then echo "No artifacts/duckdb runs found"; exit 1; fi; \
	bash ./scripts/duckdb_compare_report.sh "$$latest"

duckdb-full:
	@bash ./scripts/duckdb_experiments.sh full

july30-duckdb-gate:
	@bash ./scripts/july30_duckdb_campaign.sh gate

july30-duckdb-smallbank:
	@bash ./scripts/july30_duckdb_campaign.sh smallbank

july30-duckdb-bank-compare:
	@bash ./scripts/july30_duckdb_campaign.sh bank-compare

july30-duckdb-readheavy:
	@bash ./scripts/july30_duckdb_campaign.sh readheavy-matrix

july30-duckdb-summary:
	@bash ./scripts/july30_collect_summary.sh

july30-duckdb-full:
	@bash ./scripts/july30_duckdb_campaign.sh full

july30-duckdb-overnight-100m:
	@bash ./scripts/july30_overnight_100m.sh

july30-duckdb-overnight-40m:
	@JULY30_DUCKDB_LARGE_CARDINALITY=40000000 bash ./scripts/july30_overnight_100m.sh
