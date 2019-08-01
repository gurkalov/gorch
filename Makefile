baseRepo=gurkalov
BENCH := $(or ${bench},${bench},)
current_dir = $(shell pwd)

cpu:
	GOGC=off go test -bench='BenchmarkInit|Benchmark${BENCH}' -cpuprofile profile/cpu.out -memprofile profile/mem.prof
	go tool pprof -svg ./gorch.test ./profile/cpu.out > profile/${BENCH}Cpu.svg
	go tool pprof -svg ./gorch.test ./profile/mem.prof > profile/${BENCH}Mem.svg

test-load:
	clickhouse-client --query="truncate table events;"
	redis-cli -p 16379 del list:fast
	ab -p data.json -T application/json -c 1000 -n 100000 http://localhost:7080/add
	sleep 2;
	clickhouse-client --query="select count(*) from events;"
	redis-cli -p 16379 llen list:fast

load:
	docker run -v $(current_dir)/loadtest:/var/loadtest --net host --entrypoint /usr/local/bin/yandex-tank -it direvius/yandex-tank -c production.yaml

load-flow:
	clickhouse-client --query="truncate table events;"
	make load
	sleep 1
	clickhouse-client --query="select count(*) from events;"
	#cd loadtest && php check.php

test:
	cd packages/batcher && go test -v
	go test -v
