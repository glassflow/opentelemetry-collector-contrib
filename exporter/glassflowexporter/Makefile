include ../../Makefile.Common

.PHONY: build-example-otelcol
build-example-otelcol:
	$(MAKE) -C ../.. otelcontribcol GOOS=linux GOARCH=arm64
	cp ../../bin/otelcontribcol_linux_arm64 example/otelcontribcol
	cd example && docker compose build otelcol
