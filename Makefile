baseRepo=gurkalov
BENCH := $(or ${bench},${bench},)
current_dir = $(shell pwd)

cpu:
	GOGC=off go test -bench='BenchmarkInit|Benchmark${BENCH}' -cpuprofile profile/cpu.out -memprofile profile/mem.prof
	go tool pprof -svg ./gorch.test ./profile/cpu.out > profile/${BENCH}Cpu.svg
	go tool pprof -svg ./gorch.test ./profile/mem.prof > profile/${BENCH}Mem.svg

load:
	docker run -v $(current_dir)/loadtest:/var/loadtest --net host --entrypoint /bin/bash -it direvius/yandex-tank

test:
	cd packages/batcher && go test -v
	go test -v
