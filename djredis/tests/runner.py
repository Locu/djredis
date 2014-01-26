# coding: utf-8

import itertools
import math
import os
import subprocess
import sys
import tempfile
import time

from multiprocessing import Pipe
from multiprocessing import Process
from redis import StrictRedis


class RunnerError(Exception):
  pass


class Runner(object):
  def start(self):
    raise NotImplemented

  def stop(self):
    raise NotImplemented

  def restart(self):
    try:
      self.stop()
    except RunnerError:
      pass
    finally:
      self.start()


class ProcessRunner(Runner):
  def __init__(self, args, wait=1, verbose=False):
    """
    Runs a shell command in a separate process.
    `args` - list of args that represent the shell command.
    """
    self.args = args
    self.verbose = verbose
    self.wait = wait
    # `self._proc` is the multiprocessing.Process that executes the shell
    # command.
    # `self._sub_proc` is the subprocess.Popen `self._proc` runs for executing
    # the shell command.
    # `self._proc_pipe` is a duplex pipe connecting the parent process and
    # `self._proc`.
    self._proc = self._sub_proc = self._proc_pipe = None

  def _subprocess_target(self, pipe):
    if self.verbose:
      stdout = sys.stdout
      stderr = sys.stderr
    else:
      stdout = stderr = open(os.devnull, 'w')
    sub_proc = subprocess.Popen(self.args,
                                stdout=stdout,
                                stderr=stderr)
    # TODO(usmanm): Add some `onready` callback.
    time.sleep(self.wait)
    pipe.send(sub_proc)

  def start(self):
    if self._proc:
      raise RunnerError('Process already running.')
    self._proc_pipe, child_conn = Pipe()
    self._proc = Process(target=self._subprocess_target, args=(child_conn,))
    self._proc.start()
    self._sub_proc = self._proc_pipe.recv()
  
  def stop(self):
    if not self._proc:
      raise RunnerError('Process not running.')
    self._sub_proc.kill()
    self._proc.terminate()
    self._proc = self._sub_proc = None

class RedisRunner(ProcessRunner):
  def __init__(self, port, redis_server_path='redis-server', **kwargs):
    super(RedisRunner, self).__init__([redis_server_path,
                                       '--port', str(port)],
                                      **kwargs)


class RedisSentinelRunner(ProcessRunner):
  def __init__(self, port, masters, redis_server_path='redis-server', **kwargs):
    # Create temporary sentinel config file.
    self._tmp_file = tempfile.NamedTemporaryFile(delete=False)
    self._tmp_file.write('\n'.join(master for master in masters))
    self._tmp_file.close()
    super(RedisSentinelRunner, self).__init__([redis_server_path,
                                               self._tmp_file.name,
                                               '--sentinel',
                                               '--port', str(port)],
                                              **kwargs)

  def __del__(self):
    # Clean up temp file when the runner is being GC'd.
    try:
      os.unlink(self._tmp_file.name)
    except OSError:
      pass


class RedisRingRunner(Runner):
  MASTER_PORT = 9500
  SLAVE_PORT = 9600
  SENTINEL_PORT = 9700

  def __init__(self, redis_server_path='redis-server', num_nodes=3,
               num_sentinels=0):
    num_nodes = min(num_nodes, 15) # Don't allow more than 15 nodes
    num_sentinels = min(num_sentinels, 5) # Don't allow more than 5 sentinels
    self._masters = []
    self._slaves = []
    self._sentinels = []

    # Set up master nodes.
    for i in xrange(num_nodes):
      self._masters.append(RedisRunner(RedisRingRunner.MASTER_PORT + i,
                                       redis_server_path=redis_server_path,
                                       wait=0))

    if num_sentinels:
      masters = []
      quorum = math.ceil(num_sentinels / 2.0)
      for i in xrange(num_nodes):
        masters.append('sentinel monitor mymaster%s 127.0.0.1 %s %s' %
                       (i, RedisRingRunner.MASTER_PORT + i, quorum))
        masters.append('sentinel parallel-syncs mymaster%s 1' % i)
        masters.append('sentinel down-after-milliseconds mymaster%s 2000' % i)
        self._slaves.append(RedisRunner(RedisRingRunner.SLAVE_PORT + i,
                                        redis_server_path=redis_server_path,
                                        wait=0))
      for i in xrange(num_sentinels):
        self._sentinels.append(
          RedisSentinelRunner(RedisRingRunner.SENTINEL_PORT + i,
                              masters,
                              redis_server_path=redis_server_path,
                              wait=0)
          )

  def start(self):
    # TODO(usmanm): Make this random time.sleep less ghetto.
    for master in self._masters:
      try:
        master.start()
      except RunnerError:
        pass
    if self._sentinels:
      for slave in self._slaves:
        try:
          slave.start()
        except RunnerError:
          pass
      time.sleep(1)
      for i in xrange(len(self._slaves)):
        # Make slaves follow thier masters.
        client = StrictRedis(port=RedisRingRunner.SLAVE_PORT + i)
        client.slaveof(host='localhost', port=RedisRingRunner.MASTER_PORT + i)
      for sentinel in self._sentinels:
        try:
          sentinel.start()
        except RunnerError:
          pass
      # Give time for sentinels to reach a quorum for all masters.
      time.sleep(0.75 * len(self._masters))
    # Just wait a little to make sure everything's up.
    time.sleep(1)

  def stop(self):
    for runner in itertools.chain(self._sentinels,
                                  self._masters,
                                  self._slaves):
      try:
        runner.stop()
      except RunnerError:
        pass

  def start_master(self, i):
    assert 0 <= i < len(self._masters)
    self._masters[i].start()

  def stop_master(self, i):
    assert 0 <= i < len(self._masters)
    self._masters[i].stop()

  def start_slave(self, i):
    assert 0 <= i < len(self._slaves)
    self._slaves[i].start()

  def stop_slave(self, i):
    assert 0 <= i < len(self._slaves)
    self._slaves[i].stop()

  def start_sentinel(self, i):
    assert 0 <= i < len(self._sentinels)
    self._sentinels[i].start()

  def stop_sentinel(self, i):
    assert 0 <= i < len(self._sentinels)
    self._sentinels[i].stop()
