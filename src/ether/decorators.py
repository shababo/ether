import functools
from ether._registry import ether_pub, ether_sub

# helper decorators
ether_save = functools.partial(ether_sub, topic="Ether.save")
ether_cleanup = functools.partial(ether_sub, topic="Ether.cleanup")
