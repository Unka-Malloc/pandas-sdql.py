import os
import subprocess

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

    def write_script(self, sdql_expr):

        with open(self.script_file_path, 'w') as script:
            script.write(sdql_expr)

    def excute_script(self):
        process = self.start('sbt')
        self.write(process, f"run interpret {self.script_file_path}")
        output = self.read(process)
        self.write(process, "exit")
        self.terminate(process)
        return output

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

    def get(self, r: relation):
        self.write_script(r.sdql_expr)
        output = self.excute_script()
        return self
