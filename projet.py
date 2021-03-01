#Start a simple Spark Session
import findspark
findspark.init("C:/spark")
import pyspark 
findspark.find()

#importer des librairies
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import numpy as np
import pandas as pd
import configparser

#à partir d’un spark session réaliser ces requêtes en DSL et en SQL

#0-Créer les données

#Author

l1 = [('07890','Jean Paul Sartre'),
('05678','Pierre de Ronsard')]
rdd1 = spark.sparkContext.parallelize(l1)
Author = rdd1.toDF(['aid','name'])
Author.createOrReplaceTempView('AuthorSQL')
Author.show()

#Book

l2 = [('0001', "L'existentialisme est un humanisme", 'Philosophie'), 
      ('0002', 'Huis clos. Suivi de Les Mouches', 'Philosophie'), 
     ('0003', 'Mignonne allons voir si la rose', 'Poeme'), 
     ('0004', 'Les Amours', 'Poème')]
rdd2 = spark.sparkContext.parallelize(l2)
book = rdd2.toDF(['bid', 'title', 'category'])
book.createOrReplaceTempView('BookSQL')
book.show()

#Student


l3 = [('S15','toto','Math'),
('S16','popo','Eco'),
('S17','fofo','Mécanique') ]
rdd3 = spark.sparkContext.parallelize(l3)
Student = rdd3.toDF(['sid','sname','dept'])
Student.createOrReplaceTempView('StudentSQL')
Student.show()

#Write

l4 = [("07890","0001"),
("07890","0002"),
("05678","0003"), 
("05678","0003")]
rdd4 = spark.sparkContext.parallelize(l4) 

write = rdd4.toDF(["aid","bid"])
write.createOrReplaceTempView("WriteSQL")
write.show()

#Borrow

l5 = [('S15','0003','02-01-2020','01-02-2020'),
('S15','0002','13-06-2020','null'),
('S15','0001','13-06-2020','13-10-2020'),
('S16','0002','24-01-2020','24-01-2020'),
('S17','0001','12-04-2020','01-07-2020')]
rdd5 = spark.sparkContext.parallelize(l5) 
borrow = rdd5.toDF(['sid','bid','checkout-time','return-time']) 
borrow.createOrReplaceTempView('borrowSQL') 
borrow.show()


#1-Trouver les titres de tous les livres que l'étudiant sid='S15' a emprunté.
#SQL


#DSL

#2-Trouver les titres de tous les livres qui n'ont jamais été empruntés par un étudiant.
# SQL
spark.sql(''' select req.bid, bookSQL.title 
            from (select bid 
                from bookSQL
                where bid not in (select bid from borrowSQL) ) req
            join bookSQL on bookSQL.bid = req.bid ''').show()


# DSL
book.join(borrow,'bid', how ='left') \
    .select('bid','title') \
    .filter(F.col("checkout-time").isNull()) \
    .show()

#3-Trouver tous les étudiants qui ont emprunté le livre bid=’002’
#SQL
spark.sql(''' select bid, borrowSQL.sid, sname
            from borrowSQL 
            join StudentSQL
            on StudentSQL.sid = borrowSQL.sid
            where bid='0002' ''').show()

# DSL
borrow.join(Student,['sid']) \
    .select('bid','sid','sname') \
    .filter(F.col('bid')=='0002') \
    .show()



#4-Trouver les titres de tous les livres empruntés par des étudiants en Mécanique (département Mécanique) (ERRATUM !!)
# SQL
spark.sql(''' select req.dept, bookSQL.title 
            from (select borrowSQL.sid, bid, dept
                from borrowSQL
                join StudentSQL 
                on borrowSQL.sid = StudentSQL.sid
                where dept='Mécanique' ) req
            join bookSQL on bookSQL.bid = req.bid ''').show()



# DSL
borrow.join(Student,['sid']) \
    .join(book,['bid']) \
    .select('dept','title') \
    .filter(F.col('dept')=='Mécanique')\
    .show()

#5-Trouver les étudiants qui n’ont jamais emprunté de livre. 
# SQL
spark.sql(''' select req.sid, StudentSQL.sname
            from (select sid 
                from StudentSQL
                where sid not in (select sid from borrowSQL) ) req
            join StudentSQL on StudentSQL.sid = req.sid ''').show()

# DSL
Student.join(borrow,'sid', how ='left') \
    .select('sid','sname') \
    .filter(F.col("checkout-time").isNull()) \
    .show()

#6- Déterminer l’auteur qui a écrit le plus de livres. 
# SQL
spark.sql(''' select W.aid, A.name, count(bid) as Nb_livres
            from writeSQL as W 
            join AuthorSQL as A
            on A.aid = W.aid
            group by W.aid, A.name
            order by Nb_livres desc''').show()

# DSL
write.join(Author,'aid') \
    .select('aid','name','bid') \
    .groupBy('aid','name') \
    .agg(F.count('bid').alias('Nb_livres')) \
    .orderBy(F.col('Nb_livres').desc()) \
    .show()

#7- Déterminer les personnes qui n’ont pas encore rendu les livres.
# SQL
spark.sql(''' select borrowSQL.sid, sname, `return-time`
            from borrowSQL
            join StudentSQL
            on StudentSQL.sid = borrowSQL.sid
            where `return-time`='null' ''').show()

# DSL
borrow.join(Student,'sid') \
    .select('sid','sname','return-time') \
    .filter(F.col('return-time')=='null') \
    .show()
#8- Créer une nouvelle colonne dans la table borrow qui prend la valeur 1, si la durée d'emprunt est supérieur à 3 mois,  sinon 0.
#SQL
#???

#DSL
#????

#9-déterminer les livres qui n’ont jamais été empruntés. 
#SQL
#???


#DSL







