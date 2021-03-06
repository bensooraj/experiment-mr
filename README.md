# Map Reduce Experiment

The map reduce application logic, in this case the popular word count, is written as a golang plugin in the `plugins` directory.

### 1. Build the plugin

```sh
$ make plugin
Building the word count pluging...
go build -race -o=plugins -buildmode=plugin plugins/wc.go
```

This will spit a `*.so` file which will be used when running the workers.

### 2. Start the coordinator

```sh
$ make start-coordinator
Removing all intermediate and output files from the previous run
Starting the coordinator...
go run -race coordinator/coordinator.go data/pg-*.txt
2021/06/04 23:39:14 rpc.Register: method "Done" has 1 input parameters; needs exactly three
2021/06/04 23:39:14 rpc.Register: method "Lock" has 1 input parameters; needs exactly three
2021/06/04 23:39:14 rpc.Register: method "Unlock" has 1 input parameters; needs exactly three
```

Ignore the warnings, because the three exported methods - `Done`, `Lock` and `Unlock` - are not called via RPC.

### 3. Start the workers

You can either start a single worker,

```sh
# Build the plugin if you make any changes
$ make start-worker
```

or 3 workers in parallel,

```sh
$ make start-parallel-workers
# Update the Makefile if you need to test with more parallel workers
```

### 4. Output

The input text files are located in the `data` directory prefixed with `pg-`.

Both the intermediate and output files are written to the `output` directory.

Intermediate files of the form `mr-intermediate-x-y.json` are writted by workers after each map task.

> `x` => map task number <br /> `y` => reduce task number

The final output files (one for each reduce bucket) are written once every single map task is complete. The number of reduce buckets (currently set to 10) is controlled by the coordinator,

```golang
# coordinator/coordinator.go
m := mr.MakeCoordinator(os.Args[1:], 10)
```
