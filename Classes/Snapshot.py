#!/usr/bin/env python3

class Snapshot:
    def __init__(self, name):
        self.name = name
        self.keep_snapshot = False #Will be used during snapshot consistency check. If a snapshot has disks, it will me marked to be kept


    def __eg__(self, other):
        if not isinstance(other, Snapshot):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.name == other.name