beeline -u jdbc:hive2://localhost:10000 -n root -f create_databases.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f bronze_consumidor.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f consumidorservicosfinanceiros.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f grupoproblema.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f mediaavaliacao.hql 
beeline -u jdbc:hive2://localhost:10000 -n root -f mediaresposta.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f reclamacaotopten.hql
beeline -u jdbc:hive2://localhost:10000 -n root -f reclamacaouf.hql