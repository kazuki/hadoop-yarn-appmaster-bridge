import datetime
import os
import sys


def main():
    print('DateTime: {}'.format(datetime.datetime.now()))
    print('Python: {}'.format(sys.version.replace('\n','')))
    print('ContainerID: {}'.format(os.environ.get('CONTAINER_ID', 'UNKNOWN')))
    print()
    print('Arguments:')
    for i in range(len(sys.argv)):
        print('  {}: {}'.format(i, sys.argv[i]))
    print()
    print('Dump Environment Variables:')
    for name in os.environ:
        print('  {}: {}'.format(name, os.environ[name]))


if __name__ == '__main__':
    main()
