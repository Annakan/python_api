# -*- coding: utf-8 -*-
# NB_hosts = 3
# replication factor = 2
# forest_per_host = 2 (nb forest to create on each host for the database)
# host_path path on host to store forest datas
# data_name

# create the data base

# create the rest, security, trigger database replicated


def rotate_around(l, rotation):
    return l[rotation:] + l[:rotation - 1]

def create_database(connection, db_name, host_list, replication_factor, forest_per_host, host_path ):

#  we must have nb_hosts > min(3, replication_factor+1)

    forest_map = {}
    for index, host in enumerate(host_list):
        forest_map[host]={'host' : host,
                          'replicas' : [],
                         }
        print ("on host{} - {}".format(index, host))
        n = index+1
        replicas_hosts = rotate_around(host_list,n)
        print("-- Replicas on :", replicas_hosts)

        for host_index in range(0, len(replicas_hosts)-1, replication_factor):
            for replication_index, rep_host  in enumerate(replicas_hosts[index:index+replication_factor]):
                print("---- on host [{}] forest replica [de {}-Copie {}]".format(rep_host, host_index, replication_index+1))



if __name__ == '__main__':

    create_database("dummy", "main_data", ("host1", "host2", "host3", "host4", "host5"), replication_factor=3, forest_per_host=1,
                    host_path='')
