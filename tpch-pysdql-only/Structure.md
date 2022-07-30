pysdql takes `pysdql.Relation` as the only entrance of the programme. Users are expected to create an `pysdql.Relation` object to start all the operations. `pysdql.Relation` is actually designed as an task manager class rather than an data entity class. Its name is simply intended to give the user the illusion that they were manipulating a table or matrix. `pysdql.Relation` is responsible for receiving the data from the user and then invokes appropriate functions depending on the data type and passing it on to the real data entity class. 

`pysdql.Relation` class has many ways of receiving data from users. For example, the name of a relation is given in construction. In fact, this is the only argument required for construction.


