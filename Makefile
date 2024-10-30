deploy_all:
	mkdir -p docker-airflow/logs
	touch csv/data_hora.bst
	docker build -t consumidor-app:latest docker-app/consumidor/.
	docker compose -f docker-hadoop-spark/docker-compose.yml up -d
	docker exec hive-server bash /opt/hql/create_tables.sh
	docker compose -f docker-airflow/docker-compose.yml up -d
	docker compose -f docker-app/docker-compose.yml up -d

deploy_hadoop_spark:
	docker compose -f docker-hadoop-spark/docker-compose.yml up -d

down_hadoop_spark:
	docker compose -f docker-hadoop-spark/docker-compose.yml down

deploy_airflow:	
	docker compose -f docker-airflow/docker-compose.yml up -d 

down_airflow:	
	docker compose -f docker-airflow/docker-compose.yml down

deploy_app:
	docker compose -f docker-app/docker-compose.yml up -d

down_app:
	docker compose -f docker-app/docker-compose.yml down	

start:	
	mkdir -p docker-airflow/logs	
	docker exec hive-server bash /opt/hql/create_tables.sh
	docker build -t consumidor-app:latest docker-app/consumidor/.

create_tables:
	docker exec hive-server bash /opt/hql/create_tables.sh

build_app:
	docker build -t consumidor-app:latest docker-app/consumidor/.

deploy_postgres:
	docker compose -f docker-postgres/docker-compose.yml up -d

down_postgres:
	docker compose -f docker-postgres/docker-compose.yml down

airflow_dev:
	airflow webserver --port 9998

airflow_dev_sched:
	airflow scheduler

start_stream:
	docker run \
    	--name consumidor-app-stream-stand \
    	--network hadoop-spark \
    	--entrypoint sh \
    	--rm \
    	--detach \
    	consumidor-app:latest \
    	/app/shell/run.sh stream 1

stop_stream:
	docker container stop consumidor-app-stream-stand