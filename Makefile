format:
	go fmt github.com/bensooraj/experiment-mr/...

clean:
	@echo "Removing all intermediate and output files from the previous run"
	@find . -name "mr-intermediate-*.json" | xargs rm
	@find . -name "mr-out-*"  | xargs rm

plugin:
	@echo "Building the word count pluging..."
	@find . -name "wc.so" | xargs rm
	go build -race -o=plugins -buildmode=plugin plugins/wc.go

start-coordinator: clean
	@echo "Starting the coordinator..."
	go run -race coordinator/coordinator.go data/pg-*.txt

start-worker:
	@echo "Starting the worker..."
	@mkdir -p output
	go run -race worker/worker.go plugins/wc.so

start-parallel-workers: plugin
	@chmod u+x parallel_workers.sh
	@mkdir -p output
	@echo "Starting 3 workers in parallel..."
	@./parallel_workers.sh "go run -race worker/worker.go plugins/wc.so" "go run -race worker/worker.go plugins/wc.so" "go run -race worker/worker.go plugins/wc.so"
