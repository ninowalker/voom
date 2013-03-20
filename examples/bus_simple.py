import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..")) # make this file executable

### BEGIN
from voom.bus import VoomBus
from voom.priorities import BusPriority

bus = VoomBus()
bus.subscribe(int, lambda msg: sys.stdout.write("I see %d\n" % msg))
bus.subscribe(int, lambda msg: sys.stdout.write("squared %d\n" % msg**2), priority=BusPriority.LOW_PRIORITY)
bus.subscribe(str, lambda msg: bus.publish(int(msg[::-1])))
bus.publish(101)
bus.publish("102")