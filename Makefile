test:
	@go test -v ./... -tags="unit" -cover

test-integration:
	@go test -v ./... -tags="integration" -cover

test-unit-integration:
	@go test -parallel 10 -timeout 1m30s -v ./... -tags="unit integration" -cover

test-example:
	@go test -v ./... -tags="example" -cover

test-all:
	@go test -parallel 10 -timeout 1m30s -v ./... -tags="test" -cover

test-all-file-results:
	@touch ./test-results/test.out
	@tail -f ./test-results/test.out &
	@go test -parallel 10 -timeout 5m0s -v ./... -tags="test" -coverprofile=./test-results/coverage.txt -covermode count > ./test-results/test.out

compose: compose-infra-down compose-infra

compose-infra:
	@docker-compose -f deployments/docker-compose/dev-infra/docker-compose.yml up -d --build

compose-infra-down:
	@docker-compose -f deployments/docker-compose/dev-infra/docker-compose.yml down

compose-test:
	@docker-compose -f deployments/docker-compose/tests/docker-compose.yml up -d --build
	@echo -e "\nRabbitMQ Management URLs:"
	@echo "http://`docker-compose -f deployments/docker-compose/tests/docker-compose.yml port rabbit 15672`/#/connections"
	@echo "http://`docker-compose -f deployments/docker-compose/tests/docker-compose.yml port rabbit 15672`/#/queues"
	@echo -e ""
	@sleep 3
	@docker-compose -f deployments/docker-compose/tests/docker-compose.yml logs -f rabbeasy
	@docker-compose -f deployments/docker-compose/tests/docker-compose.yml down -v

compose-test-down:
	@docker-compose -f deployments/docker-compose/tests/docker-compose.yml down -v
