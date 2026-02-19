.PHONY: run live test clean

run:
	docker-compose up -d db
	docker-compose run --rm runner bash run.sh

live:
	USE_LIVE=1 docker-compose up -d db
	docker-compose run --rm runner bash -c "USE_LIVE=1 bash run.sh"

test:
	docker-compose up -d db
	docker-compose run --rm runner pytest -q tests/
	docker-compose run --rm runner bash -c "cd dbt && dbt test --profiles-dir ."

clean:
	docker-compose down -v
	rm -rf data/raw/*.csv dbt/target dbt/dbt_packages
