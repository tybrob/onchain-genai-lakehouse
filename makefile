.PHONY: run build up topic-enriched topic-raw down

run:
	$(MAKE) build
	$(MAKE) up

build: 
	docker-compose build

up:
	docker-compose up -d

topic-enriched:
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic btc-tx-enriched --from-beginning

topic-raw:
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic btc-mempool --from-beginning

down:
	docker-compose down