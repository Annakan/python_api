#!/usr/bin/python3

import argparse, json, logging, os, pwd, re, sys
from requests.auth import HTTPDigestAuth
from marklogic import MarkLogic
from marklogic.models.host import Host
from marklogic.models.forest import Forest
from marklogic.connection import Connection
from marklogic.exceptions import *


class CreateForests:
    def __init__(self, marklogic=None, host="localhost", hosts=None, prefix="F", forests=2, start_forest=1,
                 failover="none", failover_host=None, adminuser="admin", adminpass="admin", data_dir=None,
                 large_data_dir=None, fast_data_dir=None, database=None, dry_run=False):
        self.marklogic = marklogic
        self.host = host
        self.hosts = hosts if isinstance(hosts, list) or None else [hosts]
        self.prefix = prefix
        self.forests = forests
        self.start = start_forest
        self.failover = failover
        self.failhost = failover_host
        self.adminuser = adminuser
        self.adminpass = adminpass
        self.data_dir = data_dir
        self.large_data_dir = large_data_dir
        self.fast_data_dir = fast_data_dir
        self.database = database
        self.dry_run = dry_run

    def create_forests(self):
        conn = Connection(self.host,
                          HTTPDigestAuth(self.adminuser, self.adminpass))
        self.marklogic = MarkLogic(conn)

        if self.failhost is None and self.failover != "none":
            print("Invalid configuration, specify failover-host for failover:",
                  self.failover)
            sys.exit(1)

        if self.hosts is None:
            host_names = self.marklogic.hosts()
        else:
            host_names = self.hosts

        exists = []
        for host_index, host_name in enumerate(host_names):
            for count in range(1, self.forests + 1):
                name = self.prefix + "-" + str(host_index) + "-" + str(count)
                forest = Forest(name, host_name, conn)
                if forest.exists():
                    exists.append(host_name + ":" + name)

        if len(exists) != 0:
            print("Abort: forest(s) already exist:")
            for f in exists:
                print("   ", f)
            sys.exit(1)

        for host_index, host_name in enumerate(host_names):
            for count in range(self.start, self.start + self.forests):
                name = self.prefix + "-" + str(host_index) + "-" + str(count)
                forest = Forest(name, host_name, conn)
                if self.data_dir is not None:
                    forest.set_data_directory(self.data_dir)
                if self.large_data_dir is not None:
                    forest.set_large_data_directory(self.large_data_dir)
                if self.fast_data_dir is not None:
                    forest.set_fast_data_directory(self.fast_data_dir)

                if self.failhost is not None:
                    forest.set_failover(self.failover)
                    forest.set_failover_host_names(self.failhost)

                if self.database is not None:
                    forest.set_database(self.database)

                print("Create forest " + name + " on " + host_name)
                if self.dry_run:
                    print(json.dumps(forest.marshal(), sort_keys=True, indent=2))
                else:
                    forest.create()

        print("Finished")


def main():
    parser = argparse.ArgumentParser(description="Create forests on a cluster")

    parser.add_argument('--rest-host', default='localhost',
                        metavar='HOST', dest='marklogic',
                        help="The host to use for REST configuration")
    parser.add_argument('--host', nargs="*",
                        help='Hosts on which to create forests')
    parser.add_argument('--prefix', default='F',
                        metavar='PFX',
                        help='The prefix for forest names')
    parser.add_argument('--forests', default=2,
                        metavar='COUNT',
                        help='The number of forests per host')
    parser.add_argument('--start-forest', default=1,
                        metavar='FIRST',
                        help='The forest index at which to start numbering')
    parser.add_argument('--failover', default='none',
                        choices=['local', 'shared', 'none'],
                        help='Failover configuration')
    parser.add_argument('--failover-host',
                        metavar='HOST',
                        help='For shared-disk failover, use this failover host for all forests')
    parser.add_argument('--adminuser', default='admin',
                        help='The user for REST configuration')
    parser.add_argument('--adminpass', default='admin',
                        help='The password for REST configuration')
    parser.add_argument('--data-dir',
                        metavar='DIR',
                        help='The data directory')
    parser.add_argument('--large-data-dir',
                        metavar='DIR',
                        help='The large data directory')
    parser.add_argument('--fast-data-dir',
                        metavar='DIR',
                        help='The fast data directory')
    parser.add_argument('--database',
                        help='The database to which forests should be assigned')
    parser.add_argument('--dry-run', action='store_true',
                        help="Just print the JSON, don't actually create forests")
    parser.add_argument('--debug', action='store_true',
                        help='Turn on debug logging')

    args = vars(parser.parse_args())

    if args['debug']:
        logging.basicConfig(level=logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("marklogic").setLevel(logging.DEBUG)
    del args['debug']

    creator = CreateForests(**args)
    creator.create_forests()


if __name__ == '__main__':
    main()
