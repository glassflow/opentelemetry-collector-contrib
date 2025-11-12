FROM alpine:latest
RUN apk --no-cache add ca-certificates

# Copy a pre-built otelcontribcol binary that includes glassflowexporter
# Build this binary locally and place it next to this Dockerfile as ./otelcontribcol
COPY ./otelcontribcol /otelcontribcol
ENTRYPOINT ["/otelcontribcol"]
EXPOSE 4317 4318

