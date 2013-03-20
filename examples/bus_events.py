import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..")) # make this file executable

### BEGIN
from voom.bus import VoomBus
from voom.events.base import Event
from voom.decorators import receiver
from collections import defaultdict
import time

# our fake data stores
user_stats_db = defaultdict(lambda: defaultdict(int))
stats_db = defaultdict(int)
activity_db = defaultdict(list)

# our simplistic events
UserDidSomething = Event.new("UserDidSomething", "user thing when")
UserDidSomethingElse = Event.new("UserDidSomethingElse", "user thing when")

#
# our naive event handlers
#
@receiver(UserDidSomething)
def incrementUserActivity(event):
    user_stats_db[event.user][event.thing] += 1

@receiver(UserDidSomething)
def incrementGlobalActivity(event):
    stats_db[event.thing] += 1
    
@receiver(UserDidSomething, UserDidSomethingElse)
def appendToStream(event):
    activity_db[event.user].append(event)

@receiver(VoomBus.ALL)
def log_it(event):
    print "An event happened, yo:", event

#
# configure the bus
#
bus = VoomBus()
bus.register(incrementUserActivity)
bus.register(incrementGlobalActivity)
bus.register(appendToStream)
bus.register(log_it)

# publish some messages
bus.publish(UserDidSomething("bob", "loggedin", time.time()))
bus.publish(UserDidSomething("bob", "loggedin", time.time()))
bus.publish(UserDidSomethingElse("bob", "viewed_thing", time.time()))

assert len(activity_db["bob"]) == 3 # we see the three activities here
assert user_stats_db["bob"] == {'loggedin': 2} # but counter logic executed for only 2 of them

# publish more messages
bus.publish(UserDidSomething("alice", "loggedin", time.time()))
bus.publish(UserDidSomething("alice", "unsubscribed", time.time()))
bus.publish(UserDidSomethingElse("alice", "viewed_thing", time.time()))

assert user_stats_db["alice"] == {'loggedin': 1, 'unsubscribed': 1}

# check the global state; we only should have counted the countable events
assert stats_db == {'loggedin': 3, 'unsubscribed': 1}
assert sum(stats_db.values()) == 4

# but we recorded all events
assert sum(map(len, activity_db.values())) == 6 

### We're awesome!