deploy_all:
	mkdir docker-airflow/logs	
	docker exec hive-server bash /opt/hql/create_tables.sh
	docker build -t consumidor-app:latest docker-app/consumidor/.
	docker compose -f docker-hadoop-spark/docker-compose.yml up -d
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
	mkdir docker-airflow/logs	
	docker exec hive-server bash /opt/hql/create_tables.sh
	docker build -t consumidor-app:latest docker-app/consumidor/.

create_tables:
	docker exec hive-server bash /opt/hql/create_tables.sh

build_app:
	docker build -t consumidor-app:latest docker-app/consumidor/.