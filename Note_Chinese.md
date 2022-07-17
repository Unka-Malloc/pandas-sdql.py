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



