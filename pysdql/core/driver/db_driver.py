import os
from pysdql.core.dtypes.structure.relation import relation


class driver:
    def __init__(self, db_path, script_path=None):
        self.db_path = db_path
        self.script_path = script_path

        if self.script_path is None:
            self.script_path = os.getcwd() + fr'{os.sep}sdql_scripts'

        if not os.path.exists(self.script_path):
            os.mkdir(self.script_path)

    def write_script(self, sdql_expr):
        script_name = 'q.sdql'
        script_file_path = self.script_path + os.sep + script_name
        with open(script_file_path, 'w') as script:
            script.write(sdql_expr)

    def get(self, r: relation):
        self.write_script(r.sdql_expr)
        return self
