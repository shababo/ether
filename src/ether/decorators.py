import functools
from ._internal._registry import _ether_pub, _ether_sub

# main pub/sub decorators
ether_pub = _ether_pub
ether_sub = _ether_sub

# partials for subscribing to ether-lifecycle topics
ether_init = functools.partial(ether_sub, topic="Ether.init")
ether_save = functools.partial(ether_sub, topic="Ether.save")
ether_cleanup = functools.partial(ether_sub, topic="Ether.cleanup")