import asyncio
import sys

from asynciorpc.client import Client

class AppMaster(object):
    def __init__(self, bridge_port):
        self.loop = asyncio.get_event_loop()
        self.client = Client('127.0.0.1', bridge_port,
                             notification_handler=self._yarn_notification)
        self.launched_containers = 0
        self.requested_containers = self.wait_containers = 100
        self.sh_name = sys.argv[2][sys.argv[2].rindex('/')+1:]

    def start(self):
        for _ in range(self.wait_containers):
            self.client.call('addContainerRequest',
                             cpu=1, memory=512, priority=0)
        try:
            self.loop.run_forever()
        except:
            pass
        self.loop.close()
        print('[AM] terminated')

    def _onContainersAllocated(self, containers):
        for c in containers:
            if self.launched_containers == self.requested_containers:
                fut = self.client.call_async(
                    'releaseAssignedContainer',
                    container_id=c['container_id'])
                fut.add_done_callback(lambda _: None)
                continue
            self.launched_containers += 1
            fut = self.client.call_async(
                'startContainer', container_id=c['container_id'],
                command='/bin/bash ' + self.sh_name)
            fut.add_done_callback(lambda _: None)
            print('[AM] launch {} ({}/{})'.format(c['container_id'],
                                                  self.launched_containers,
                                                  self.requested_containers))

    def _onContainersCompleted(self, statuses):
        self.wait_containers -= len(statuses)
        for s in statuses:
            print('[AM] {} terminated'.format(s['container_id']))
        if self.wait_containers <= 0:
            print('[AM] terminating...')
            self.loop.stop()
        else:
            print('[AM] waiting containers {}/{}'.format(
                self.wait_containers, self.requested_containers))

    def _yarn_notification(self, name, *args, **kwargs):
        if name == 'onContainersAllocated':
            self._onContainersAllocated(**kwargs)
        elif name == 'onContainersCompleted':
            self._onContainersCompleted(**kwargs)


def main():
    am = AppMaster(int(sys.argv[1]))
    am.start()


if __name__ == '__main__':
    main()
