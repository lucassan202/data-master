deploy_hadoop_spark:
	docker compose -f docker-hadoop-spark/docker-compose.yml up

down_hadoop_spark:
	docker compose -f docker-hadoop-spark/docker-compose.yml down

deploy_airflow:	
	docker compose -f docker-airflow/docker-compose.yml up

down_airflow:	
	docker compose -f docker-airflow/docker-compose.yml down

start:	
	mkdir docker-airflow/logs	
	docker exec hive-server bash /opt/hql/create_tables.sh
	docker build -t consumidor-app:1.0.0 docker-hadoop-spark/consumidor/.