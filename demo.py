import pysdql

if __name__ == '__main__':
    print(pysdql.get_config())
    pysdql.set_verify(False)
    print(pysdql.get_config())
