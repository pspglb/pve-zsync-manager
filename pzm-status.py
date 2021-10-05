#!/usr/bin/env python3

import pzm-common
from prettytable import PrettyTable
from json.decoder import JSONDecodeError


#Colors for fancy table output
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

#Read status from json status file. Either in fancy, human friendly manner (plain=False), or for automated reports, in plain text
def read_from_json(plain):
    if not os.path.exists(pzm-common.statusJsonFile):
        os.mknod(pzm-common.statusJsonFile)
    with open(pzm-common.statusJsonFile, "r") as jsonFile:
        try:
            readdata = json.load(jsonFile)
            readdataString = json.dumps(readdata, sort_keys=True)
            readdata = json.loads(readdataString)
            lines = []
            headers=["VM/CT-ID", "Backupname", "Starttime", "Endtime", "Duration", "Size", "Status", "Additional Info"]
            empty_line = []
            for header in headers:
                empty_line.append("")
            # Sort by Backupname
            sorted = {}
            for name, data in readdata.items():
                if not data['backupname'] in sorted:
                    sorted[data['backupname']] = {}
                sorted[data['backupname']][name] = data

            if plain:
                lines = []
                for name, data in sorted.items():
                    for name, data in data.items():
                        line = []
                        line.append(data['id'])
                        line.append(data['backupname'])
                        line.append(data['starttime'])
                        line.append(data['endtime'])
                        line.append(data['duration'])
                        if (data.get('size') is not None):
                            line.append(data['size'])
                        else:
                            line.append("")
                        line.append(data['status'])
                        line.append(data['info'])
                        lines.append(line)
                    lines.append(empty_line)
                format_row = "{:<10} {:<22} {:<21} {:<21} {:<16} {:<8} {:<8} {:<30}"
                print (format_row.format(*headers))
                lines.pop() # remove last item - empty line

                for line in lines:
                    print(format_row.format(*line))

            else:
                for i in range(len(headers)):
                    headers[i] = bcolors.HEADER + headers[i] + bcolors.ENDC

                table = PrettyTable(headers)

                for name, data in sorted.items():
                    for name, data in data.items():
                        table.add_row([(bcolors.BOLD if data['id'] == "all" else "") + data['id'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['backupname'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['starttime'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['endtime'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['duration'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['size'] if data.get('size') is not None else "-" + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + (bcolors.FAIL if data['status'] == "error" else bcolors.OKGREEN) + data['status'] + bcolors.ENDC + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['info'] + (bcolors.ENDC if data['id'] == "all" else "")
                                     ])
                    table.add_row(empty_line)
                row_count = 0
                for row in table:
                    row_count += 1
                table.del_row(row_count -1 )
                print (table)
        except JSONDecodeError:
            return
