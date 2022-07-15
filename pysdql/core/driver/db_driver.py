import os
import subprocess

from pysdql.core.dtypes.GroupbyExpr import GroupbyExpr
from pysdql.core.dtypes.structure.relation import relation


class driver:
    def __init__(self, db_path, script_path=None):
        self.db_path = db_path
        self.script_path = script_path

        if self.script_path is None:
            self.script_path = os.getcwd() + fr'{os.sep}sdql_scripts'
            if not os.path.exists(self.script_path):
                os.mkdir(self.script_path)

        self.script_file_name = 'q.sdql'
        self.script_file_path = (self.script_path + os.sep + self.script_file_name).replace('\\', '/')

        self.output = ''

    def start(self, cmd):
        return subprocess.Popen(
            cmd,
            shell=True,
            cwd=self.db_path,
            text=True,
            encoding='utf-8',
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

    @staticmethod
    def read(process: subprocess):
        output = []
        get_output = False

        line = process.stdout.readline().strip()
        while line:
            print(line)

            if '[success] Total time:' in line:
                break
            if '[error] Total time:' in line:
                break

            if get_output:
                output.append(line)
            if '[info] running sdql.driver.Main interpret' in line:
                get_output = True

            line = process.stdout.readline().strip()

        return output

    @staticmethod
    def write(process: subprocess, cmd: str):
        process.stdin.write(f"{cmd}\n")
        process.stdin.flush()

    @staticmethod
    def terminate(process: subprocess):
        process.stdin.close()
        process.terminate()
        process.kill()
        process.wait(timeout=0.2)

    def write_script(self, sdql_expr):
        with open(self.script_file_path, 'w') as script:
            script.write(sdql_expr)
            script.close()

    def excute_script(self):
        process = self.start('sbt')
        self.write(process, f"run interpret {self.script_file_path}")
        output = self.read(process)
        self.write(process, "exit")
        self.terminate(process)
        print('========================================================')
        return output

    def run(self, query, show=True, block=False):
        if type(query) == relation or type(query) == GroupbyExpr:
            query = query.sdql_expr
        elif type(query) == str:
            query = query

        if show:
            print('========================================================')
            print(query)
            print('========================================================')

        if block:
            return self

        self.write_script(query)
        self.output = self.excute_script()

        return self

    def export(self, data=None, file_name='query'):
        file_path = (self.script_path + os.sep + file_name + '.sdql').replace('\\', '/')
        if type(data) == relation or type(data) == GroupbyExpr:
            with open(file_path, 'w') as f:
                f.write(data.sdql_expr)
        print(f'export sdql script {file_name}.sdql to {file_path} \n'
              f'excute by: \n'
              f'run interpret {file_path}')

    def __repr__(self):
        return ''.join(self.output)
