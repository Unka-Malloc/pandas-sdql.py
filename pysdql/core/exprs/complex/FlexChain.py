from pysdql.core.exprs.advanced.ColAlterExprs import (
    NewColInsert,
    OldColRename
)
from pysdql.core.exprs.advanced.ColOpIsinExpr import ColOpIsin
from pysdql.core.exprs.advanced.ColProjExprs import ColProj
from pysdql.core.exprs.advanced.MergeExprs import MergeExpr
from pysdql.core.exprs.complex import (
    AggrExpr,
    GroupbyAggrExpr
)


class OpChain:
    def __init__(self, node):
        """

        :param node:

        Attributes:
            peer:
                Another node that directly connected to the current node.
                It is a safety measure that when any inference fails,
                    the structure of the two sub-graphs can be obtained from peer.
            connect:
                The relavant connection between nodes.
            endpoint:
                The current node will be no longer needed
                    after a certain operation, which represents the end of the node's life cycle.
                The endpoint is always a transit node.
            redirect:
                The current node is a transit station
                    and need to redirect to the position that iteration really happens.
            terminal:
                More powerful redirect,
                    go to the node that iteration really happens.
        """
        self.__node = node
        self.__forward = []
        self.__backward = []

        self.transit = False
        self.peer = None
        self.peer_link = None
        self.connect = {}

        self.entrance = False
        self.endpoint = None
        self.redirect = None
        self.terminal = None

        self.indices = []

        self.attrout = self.infer_attrout()
        self.dpdcpool = self.retriever.findall_cols_used(as_owner=True, only_next=True)

        self.fill()
        self.fill_terminal()

    @property
    def node(self):
        return self.__node

    @property
    def retriever(self):
        return self.node.retriever

    @property
    def forward(self):
        return self.__forward

    @forward.setter
    def forward(self, value: list):
        self.__forward = value

    @property
    def backward(self):
        return self.__backward

    @backward.setter
    def backward(self, value: list):
        self.__backward = value

    def find_cache(self, which, node):
        if hasattr(self, which):
            for o in getattr(self, which):
                if hasattr(node, 'oid') and hasattr(o, 'oid'):
                    if node.oid == o.oid:
                        return o

        return None

    def find_forward(self, node):
        return self.find_cache('forward', node)

    def find_backward(self, node):
        return self.find_cache('backward', node)

    def add_forward(self, target):
        self.__forward.append(target)

    def add_backward(self, target):
        self.__backward.append(target)

    def del_forward(self, target):
        self.__forward = self.remove(self.__forward, target)

    def del_backward(self, target):
        self.__backward = self.remove(self.__backward, target)

    @staticmethod
    def remove(old_list, target):
        new_list = []
        for o in old_list:
            if not OpChain.equals(target, o):
                new_list.append(o)
        return new_list

    @staticmethod
    def equals(e1, e2) -> bool:
        if hasattr(e1, 'oid') and hasattr(e2, 'oid'):
            return e1.oid == e2.oid
        else:
            raise AttributeError(f'Cannot compare objects without oid.')

    def add_build(self, target):
        if 'build' in self.connect.keys():
            self.connect['build'].append(target)
        else:
            self.connect['build'] = [target]

    def add_probe(self, target):
        if 'probe' in self.connect.keys():
            self.connect['probe'].append(target)
        else:
            self.connect['probe'] = [target]

    def add_joint(self, target):
        if 'joint' in self.connect.keys():
            self.connect['joint'].append(target)
        else:
            self.connect['joint'] = [target]

    def fill(self):
        if not hasattr(self.node, 'op_stack'):
            raise AttributeError(f'Cannot infer object without op_stack')

        for op_expr in self.node.op_stack:
            op_body = op_expr.op
            if isinstance(op_body, MergeExpr):
                if self.equals(self.node, op_body.left):
                    self.add_forward(op_body.joint)

                    self.peer = op_body.right
                    self.peer_link = (op_body.left_on, op_body.right_on)

                    self.add_probe(op_body.right)

                    self.add_joint(op_body.joint)

                if self.equals(self.node, op_body.right):
                    self.add_forward(op_body.joint)

                    self.peer = op_body.left
                    self.peer_link = (op_body.right_on, op_body.left_on)

                    self.add_build(op_body.left)

                    if not self.node.is_joint:
                        self.terminal = self.node

                    self.add_joint(op_body.joint)

                if self.equals(self.node, op_body.joint):
                    self.add_backward(op_body.left)
                    self.add_backward(op_body.right)

                    self.transit = True

                    self.redirect = op_body.right

            if isinstance(op_body, ColOpIsin):
                if self.equals(self.node, op_body.part_on):
                    self.add_forward(op_body.probe_on)

                    self.peer = op_body.probe_on
                    self.peer_link = (op_body.col_part.field, op_body.col_probe.field)

                    self.add_probe(op_body.probe_on)
                if self.equals(self.node, op_body.probe_on):
                    self.add_backward(op_body.part_on)

                    self.peer = op_body.part_on

                    self.add_build(op_body.part_on)
                    self.peer_link = (op_body.col_probe.field, op_body.col_part.field)

                    self.redirect = op_body.probe_on
                    self.terminal = op_body.probe_on

    def fill_terminal(self):
        if self.redirect and not self.equals(self.node, self.redirect):
            next_chain = self.redirect.get_op_chain()
            self.terminal = next_chain.terminal

        if not self.terminal:
            if self.peer:
                peer_chain = self.peer.get_op_chain()
                self.terminal = peer_chain.terminal

    def infer_attrout(self):
        output_attr = []

        rename_attr = {}

        for op_expr in self.node.op_stack:
            op_body = op_expr.op

            if isinstance(op_body, NewColInsert):
                output_attr.append(op_body.col_var)
            elif isinstance(op_body, OldColRename):
                if isinstance(op_body.col_expr, str):
                    rename_attr[op_body.col_var] = op_body.col_expr
            elif isinstance(op_body, ColProj):
                output_attr = op_body.proj_cols
            elif isinstance(op_body, AggrExpr):
                output_attr = list(op_body.aggr_op.keys())
            elif isinstance(op_body, GroupbyAggrExpr):
                output_attr = op_body.groupby_cols + list(op_body.origin_dict.keys())

        res = output_attr if output_attr else self.node.cols_in

        for a in res:
            if a in rename_attr.keys():
                res[res.index(a)] = rename_attr[a]

        return res if res else self.node.cols_in

    def find_path(self):
        pass

    def find_dpdc(self, dependency, pure=True):
        if self.transit:
            for sub_node in self.backward:
                sub_chain = sub_node.get_op_chain()

                if self.equals(sub_node, self.terminal):
                    if dependency in sub_chain.attrout:
                        return (dependency, [(sub_node, sub_chain.peer_link[0])])
                else:
                    if sub_chain.transit:
                        return sub_chain.find_dpdc(dependency=dependency, pure=pure)
                    else:
                        if dependency in sub_chain.attrout:
                            if self.equals(sub_chain.peer, self.terminal):
                                return (dependency, [(sub_chain.peer, sub_chain.peer_link[1]), (sub_node, sub_chain.peer_link[0])])
                            else:
                                return sub_chain.peer.get_op_chain().find_dpdc(dependency=dependency, pure=pure)
        else:
            if dependency in self.attrout:
                return (dependency, [(self.node, self.peer_link[0])])

        return (dependency, [])


    def infer(self, entrance=False):
        self.entrance = entrance

        if self.transit:
            for i in range(len(self.backward)):
                sub_node = self.backward[i]
                sub_chain = sub_node.get_op_chain()

                if self.equals(self.terminal, sub_node):
                    continue

                if sub_chain.transit:
                    if self.equals(sub_chain.terminal, self.terminal):
                        self.indices += sub_chain.indices
                    else:
                        self.indices.append(sub_node)
                else:
                    self.indices.append(sub_node)

                sub_chain.infer()
        else:
            for i in range(len(self.backward)):
                sub_node = self.backward[i]
                sub_chain = sub_node.get_op_chain()
                sub_chain.infer()

        print(self)

        return self

    def __repr__(self):
        res = [
            f'node: {self.node} {{',
            f'  entrance -> {self.entrance}',
            f'  dependency_pool -> {self.dpdcpool}',
            f'  attr out -> {self.attrout}',
            f'  indices -> {self.indices}',
            f'  terminal -> {self.terminal}',
            f'  peer -> {self.peer} | {self.peer_link}',
            '}'
        ]

        return '\n'.join(res)