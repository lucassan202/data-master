beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/hql/create_databases.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/hql/bronze_consumidor.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/hql/consumidorservicosfinanceiros.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/hql/grupoproblema.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/hql/mediaavaliacao.hql 
beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/hql/mediaresposta.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/hql/reclamacaotopten.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/hql/reclamacaouf.hql