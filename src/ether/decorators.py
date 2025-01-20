import functools
from ._internal._registry import _ether_pub, _ether_sub, _ether_save, _ether_get

# main pub/sub decorators
ether_pub = _ether_pub
ether_sub = _ether_sub
ether_save = _ether_save
ether_get = _ether_get

# partials for subscribing to ether-lifecycle topics
ether_init = functools.partial(ether_sub, topic="Ether.init")
ether_save_all = functools.partial(ether_sub, topic="Ether.save_all")
ether_start = functools.partial(ether_sub, topic="Ether.start")
ether_cleanup = functools.partial(ether_sub, topic="Ether.cleanup")
ether_shutdown = functools.partial(ether_sub, topic="Ether.shutdown")