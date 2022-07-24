import logging
import os
import subprocess
import time

from pysdql.core.dtypes.GroupbyExpr import GroupbyExpr
from pysdql.core.dtypes.relation import relation


class db_driver:
    def __init__(self, db_path, name='', script_path=None):
        self.db_path = db_path
        self.script_path = script_path

        if self.script_path is None:
            self.script_path = os.getcwd() + fr'{os.sep}output'
            if not os.path.exists(self.script_path):
                os.mkdir(self.script_path)

        self.script_file_name = 'Q00.sdql'
        self.script_file_path = (self.script_path + os.sep + self.script_file_name).replace('\\', '/')

        self.output = ''
        self.data = None

        if name:
            self.driver_name = name
        else:
            self.driver_name = 'db_driver'

        self.logger = self.logger_config(self.driver_name)

    @staticmethod
    def logger_config(log_name, log_path=None):
        if log_path is None:
            log_path = os.getcwd() + fr'{os.sep}output' + fr'{os.sep}log'
            if not os.path.exists(log_path):
                os.mkdir(log_path)

        file_name = f'{log_name}.log'

        logger = logging.getLogger(log_name)
        logger.setLevel(level=logging.DEBUG)
        handler = logging.FileHandler(log_path + os.sep + file_name, encoding='UTF-8')

        # '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter('[%(levelname)s]%(asctime)s: %(message)s')
        handler.setFormatter(formatter)

        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)

        logger.addHandler(handler)
        logger.addHandler(console)
        return logger

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

    def read(self, process: subprocess):
        output = []
        get_output = False

        line = process.stdout.readline().strip()
        while line:
            self.logger.info(line)

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

    def excute_script(self, skip_banner=False):
        if skip_banner:
            process = self.start('sbt skipBanner')
        else:
            process = self.start('sbt')
        self.write(process, f"run interpret {self.script_file_path}")
        output = self.read(process)
        self.write(process, "exit")
        self.terminate(process)
        self.logger.info('========================================================')
        return output

    def run(self, query, show=True, skip_banner=False, verbose=False, mute=False, block=False):
        t1 = time.time()

        if type(query) == relation or type(query) == GroupbyExpr:
            query = query.sdql_expr
        elif type(query) == str:
            query = query

        self.data = query

        if show:
            self.logger.info('========================================================')
            self.logger.info(query)
            self.logger.info('========================================================')

        if block:
            return self

        self.write_script(query)
        self.output = self.excute_script(skip_banner=skip_banner)

        self.logger.info(f'pysdql execution time: {time.time() - t1}')
        return self

    def export(self, file_name='', data=None):
        if file_name:
            file_name = file_name
        else:
            if self.driver_name != 'db_driver':
                file_name = self.driver_name + '.sdql'
            else:
                file_name = 'query.sdql'

        if data is None:
            data = self.data

        file_path = (self.script_path + os.sep + file_name).replace('\\', '/')
        if type(data) == relation or type(data) == GroupbyExpr:
            with open(file_path, 'w') as f:
                f.write(data.sdql_expr)
        else:
            if data:
                with open(file_path, 'w') as f:
                    f.write(str(data))

        self.logger.info(f'export sdql script "{file_name}" to "{file_path}"')
        self.logger.info(f'excute by:')
        self.logger.info(f'run interpret {file_path}')

        return self

    def red(self, file_name=''):
        if file_name:
            file_name = file_name
        else:
            if self.driver_name != 'db_driver':
                file_name = self.driver_name + '.out'
            else:
                file_name = 'query.out'

        with open(self.script_path + os.sep + file_name, 'w') as f:
            f.write(''.join(self.output))
        return self

    def to(self, file_name=''):
        return self.red(file_name)

    def __repr__(self):
        return ''.join(self.output)
