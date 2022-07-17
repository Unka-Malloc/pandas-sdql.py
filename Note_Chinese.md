### Composition

`ColUnit` (`+`, `-`, `*`, `/`) `ColUnit` `=>` `ColUnit`

`ColUnit` (`>`, `>=`, `==`, `<=`, `<=`, `!=`) `scalar` `=>` `CondUnit`

`scalar` :: `int` | `float` | `str` 

`CondUnit` (`&`, `|`) `CondUnit` `=>` `CondUnit`

`~` `CondUnit` `=>` `CondUnit`

`relation` `.exists()` `=>` `CondUnit`

`ColUnit` `.isin()` `=>` `CondUnit`

(`CondUnit`, `then_case`, `else_case`) `=>` `CondExpr`

`then_case` | `else_case` :: `DictExpr` | `RecExpr` | `bool` | `int` | `float` | `str` 

`ColUnit -> CondUnit -> CondExpr -> selection()`

Task manager (relation) 根据子元素的固有属性来进行管理

[comment]: <> (run interpret progs/test.sdql)

sum(<d_k, d_v> in data) if (0 < (sum (<x_k, x_v> in ref) if(d_k.o_orderkey == x_k.l_orderkey) then x_v else 0)

## export DSS_QUERY=queries

其中queries文件夹是你的查询语句的模板所在的文件夹，可以自己修改
再次运行qgen -d后将select语句生成并输出到了控制台，我们只需将其重定向到文件中即可。

13. Environment Variables

Enviroment variables are used to control features of DBGEN and QGEN 
which are unlikely to change from one execution to another.

Variable    Default     Action
-------     -------     ------
DSS_PATH    .           Directory in which to build flat files
DSS_CONFIG  .           Directory in which to find configuration files
DSS_DIST    dists.dss   Name of distribution definition file
DSS_QUERY   .           Directory in which to find query templates


sbt -mem 2048 

