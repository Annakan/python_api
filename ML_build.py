# -*- coding: utf-8 -*-
# NB_hosts = 3
# replication factor = 2
# forest_per_host = 2 (nb forest to create on each host for the database)
# host_path path on host to store forest data
# data_name

from collections import OrderedDict
from copy import deepcopy
import json

FS_NAME= "{}-{}F{:02d}"
F_NAME="{}-{}"


# create the data base

# create the rest, security, trigger database replicated

def rotate(l, rotation):
    return l[rotation:] + l[:rotation]

def forest_to_dispatch(forest_to_remove, forest_list):
    result = forest_list[:]
    for forest in forest_to_remove:
        result.remove(forest)
    return result

def generate_forests(db_name, host_list, forest_per_host, replication_factor):
    data_forests = OrderedDict()
#    forest_replicas_flat = []
    forest_replica_dic = OrderedDict()
    for host in host_list:
        data_forests[host]=[]
        forest_replica_dic[host]=[]
        for i in range(1,forest_per_host+1):
            forest_name = FS_NAME.format(db_name, host, i) if forest_per_host > 1 else F_NAME.format(db_name, host)
            data_forests[host].append(forest_name)
            for replica_index in range(1,replication_factor+1):
                replica_name = "{}{:02d}-{}".format("RE", replica_index, forest_name) if replication_factor>1 else\
                               "{}-{}".format("RE", forest_name)
#                forest_replicas_flat.append(replica_name)
                forest_replica_dic[host].append(replica_name)
    stride = forest_per_host*replication_factor
#    print("forest_replicas_flat", forest_replicas_flat)
    print("stride", stride)
    forest_replicas = OrderedDict()
    for repl_count in range(replication_factor):
        for host_num, host in enumerate(host_list):
            replica_bag = deepcopy(forest_replica_dic)
            del(replica_bag[host])
            # print(replica_bag)
            replicas = forest_replicas[host] if host in forest_replicas else []
            for j, cur_host in enumerate(replica_bag):
                #pos = j*stride + repl_count
                replicas.append(replica_bag[cur_host][(repl_count+j) % forest_per_host ])
            forest_replicas[host] = replicas
    # for repl_count in range(replication_factor):
    #     for host_num, host in enumerate(host_list):
    #         replicas_bag = forest_to_dispatch(forest_replica_dic[host],forest_replicas_flat)
    #         print(replicas_bag)
    #         replicas = forest_replicas[host] if host in forest_replicas else []
    #         for j in range(forest_per_host):
    #             print(j % forest_per_host)
    #             pos = j*stride + repl_count + j % forest_per_host
    #             replicas.append(replicas_bag[pos])
    #         forest_replicas[host] = replicas

    # replicas_ring = rotate(forest_replicas_flat, stride)
    # for repl_count in range(replication_factor):
    #     for host_num, host in enumerate(host_list):
    #         print("replicas_ring [{}] : {}".format(host_num,replicas_ring))
    #         replicas=forest_replicas[host] if host in forest_replicas else []
    #         for j in range(forest_per_host):
    #             pos = j*stride+ (repl_count + j) % forest_per_host
    #             replicas.append(replicas_ring[pos])
    #         forest_replicas[host] = replicas
    #         replicas_ring = rotate(replicas_ring, stride)
    #     replicas_ring = rotate(replicas_ring, replication_factor)
    return data_forests, forest_replicas

def create_database(connection, db_name, host_list, replication_factor, forest_per_host, host_path ):
    #  we must have nb_hosts > min(3, replication_factor+1)

    forest_list, forest_replica_dict =\
    generate_forests("Data", host_list, replication_factor=replication_factor, forest_per_host=forest_per_host)

    script = OrderedDict()
    for host in forest_list:
        host_code = OrderedDict({
            "host" : host,
            "path" : host_path,
            "forest-replicas": forest_replica_dict[host]
        })
        script[host] = host_code

    return  script


from marklogic import MarkLogic
from marklogic.connection import Connection
from requests.auth import HTTPDigestAuth

def build_cluster(host, adminuser, adminpass, couple_user, couple_pass, cluster_host_list):
    conn = Connection(host, HTTPDigestAuth(adminuser, adminpass))
    marklogic = MarkLogic(conn)

    if cluster_host_list is not None:
        for couple in cluster_host_list:
            print("{0}: couple with {1}...".format(host, couple))
            altconn = Connection(couple,
                                 HTTPDigestAuth(couple_user,
                                                couple_pass))
            altml = MarkLogic(altconn)
            altcluster = altml.cluster()
            cluster = marklogic.cluster()
            cluster.couple(altcluster)

    print("Finished")



if __name__ == '__main__':

    script = create_database("dummy", "main_data", ("host1", "host2", "host3", "host4", "host5"), replication_factor=1, forest_per_host=1,
                   host_path='')
    print(script)
    print(json.dumps(script))

    # forest_list, forest_replica_dict =\
    #     generate_forests("Data", ("host1", "host2", "host3"), # "host4", #, "host5"),
    #                     replication_factor=2, forest_per_host=2)
    #
    #
    #
    # print("Data Forests")
    # print(forest_list)
    # print("Replicas dic")
    # print(forest_replica_dict)

    # Launch setup-scripts for the OS
    # Install ML
    # Initialize ML
    # https://docs.marklogic.com/REST/POST/admin/v1/init

    # Create cluster
    # https://docs.marklogic.com/REST/POST/admin/v1/cluster-config

    # Create Database
    # https://docs.marklogic.com/REST/POST/manage/v2/databases
