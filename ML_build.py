# -*- coding: utf-8 -*-
# NB_hosts = 3
# replication factor = 2
# forest_per_host = 2 (nb forest to create on each host for the database)
# host_path path on host to store forest datas
# data_name

from collections import OrderedDict

# create the data base

# create the rest, security, trigger database replicated

def rotate(l, rotation):
    return l[rotation:] + l[:rotation]


def generate_forests(db_name, host_list, forest_per_host, replication_factor):

    data_forests = OrderedDict()
    forest_replicas_flat = []
    for host in host_list:
        data_forests[host]=[]
        for i in range(1,forest_per_host+1):
            forest_name = "{}-{}F{:02d}".format(db_name,host, i)
            data_forests[host].append(forest_name)
            for replica_index in range(1,replication_factor+1):
                replica_name = "{}-{}{:02d}".format(forest_name,"Re", replica_index)
                forest_replicas_flat.append(replica_name)
    stride = forest_per_host*replication_factor
    print("forest_replicas_flat", forest_replicas_flat)
    print("stride", stride)
    replicas_ring = rotate(forest_replicas_flat, stride)
    forest_replicas = OrderedDict()
    for repl_count in range(replication_factor):
        for host_num, host in enumerate(host_list):
            print("replicas_ring [{}] : {}".format(host_num,replicas_ring))
            replicas=forest_replicas[host] if host in forest_replicas else []
            for j in range(forest_per_host):
                pos = j*stride
                replicas.extend(replicas_ring[pos:pos+1])
            forest_replicas[host] = replicas
            replicas_ring = rotate(replicas_ring, stride)
        replicas_ring = rotate(replicas_ring, replication_factor)
    return  data_forests, forest_replicas_flat, forest_replicas

# def create_database(connection, db_name, host_list, replication_factor, forest_per_host, host_path ):
    #  we must have nb_hosts > min(3, replication_factor+1)

    #
    # forest_map = {}
    # for index, host in enumerate(host_list):
    #     forest_map[host]={'host' : host,
    #                       'replicas' : [],
    #                      }
    #     print ("on host{} - {}".format(index, host))
    #     n = index+1
    #     replicas_hosts = rotate_around(host_list,n)
    #     print("-- Replicas on :", replicas_hosts)
    #
    #     for host_index in range(0, len(replicas_hosts)-1, replication_factor):
    #         for replication_index, rep_host  in enumerate(replicas_hosts[index:index+replication_factor]):
    #             print("---- on host [{}] forest replica [de {}-Copie {}]".format(rep_host, host_index, replication_index+1))



if __name__ == '__main__':

    # create_database("dummy", "main_data", ("host1", "host2", "host3", "host4", "host5"), replication_factor=3, forest_per_host=1,
    #                host_path='')

    forest_list, replica_list, forest_replica_dict =\
        generate_forests("Data", ("host1", "host2", "host3", "host4", "host5"),
                        replication_factor=2, forest_per_host=1)

    print("Data Forests")
    print(forest_list)
    print("replicas flat list")
    print(replica_list)
    print("Replicas dic")
    print(forest_replica_dict)


