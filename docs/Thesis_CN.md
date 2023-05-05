## Parser
Even though a Parser class does not exists in pd2sd package, there is a parsing procedure, which will dynamically create, compose, and link different operation nodes based on the semantic of pandas functions. 

The parsing is highly depends on overloading the Python built-in methods and operators. Each pandas function can be mapped to single or multiple semantics based on the way of using it. Therefore, each pandas function will be parsed to one or several operation nodes. Normally, if an operation involves more than one DataFrame object, then the operation node will be added to all relevant DataFrame objects simultaneously.

语义映射并不代表pandas内的每一个类都在pd2sd包内有一个对应的类或者方法. 恰恰相反, 语义所对应的操作节点和其对应的pandas是完全不同的. 例如, DataFrame 是 pandas 的一个类, 它表示一个类似表格的数据对象. 但是在pd2sd解析过程中, DataFrame会被解析为一个类表格对象, 创建它的操作将会是TableInit操作节点, 而复制的语义则由TableCopy操作节点. 这些操作节点都会被添加进其对应的TableMain栈中, 而这些都和一个DataFrame对象完全不同. 

和语义对象不同的是, pd2sd 内置了代表数据的数据对象, 对应了pandas的DataFrame, DataFrameGroupBy, Series类. 这些数据对象和其对应的pandas类拥有完全一样的类名称和方法. 当用户使用这些类的方法时, 这些类将会在内部隐式的生成操作节点并构成查询图. 这样做的目的是通过对用户隐藏所有的解析和优化过程, 而使查询树的生成更加可控和安全. 受限于时间的原因, 依然有非常多的方法直接作用在语法对象上而不是数据对象. 尽管这种架构改动并不会影响性能, 但在未来的代码优化中应该纳入考量. 

## Optimizer

## Testing
测试章节分为两个部分: 集成测试和性能测试. 单元测试并不适用于这个项目, 这是由于项目绝大多数类和模组表示语义操作节点, 而单个的操作节点基本上就是一些特定信息的集合. 只有在使用语义物体进行查询生成的时候, 才能检测单个对象的表达是否符合预期. 
