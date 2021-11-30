#!/usr/bin/env python3

from Classes.Disk import Disk

class Non_Backed_Up_Disk(Disk):
    def __init__(self, config_file, config_line, type):
        super().__init__()
        self.unique_name = Disk.get_unique_name_from_config_line(config_line)
        self.type = type
        self.config_line = config_line
        self.destination = self.get_destination()
        self.recreate = False