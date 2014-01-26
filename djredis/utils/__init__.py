from redis.sentinel import SentinelConnectionPool


def get_node_name(node):
  if isinstance(node.connection_pool, SentinelConnectionPool):
    return node.connection_pool.service_name
  return '%s:%s' % (node.connection_pool.connection_kwargs['host'],
                    node.connection_pool.connection_kwargs['port'])
