'''Protocol layer clients for remote bup.

Servers wrap bup-clients to allow them to operate over potentially lossy,
delayed or otherwise possibly unreliable networks.
'''

# This tuple should list the UUIDs of server classes implemented here or
# otherwise available. Clients can use this to determine if a given bup server
# has the capabilities they need.
CAPABILITY = ('Classic',
              'Binary_v1')