#!/usr/bin/env python3
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.topo import Topo

NODES = [
    "A9460D5E6B9A0",
    "A9460D5E6B940",
    "A9460D5E65E00",
    "A9460D5E66D00",
    "A9460D5E0EC80",
]

# CONFIGURATIONS
BANDWIDTH = 1000
DELAY = '1ms'

class SpineLeaf(Topo):
    def __init__(self, n_spine=2, n_leaf=3, n_host=3, use_real_dpid = True, **opts):
        super(SpineLeaf, self).__init__(**opts)
        spines = []
        leafs = []
        # Add spine switches
        for i in range(1, n_spine + 1):
            spine = self.addSwitch(f's{i}', protocols='OpenFlow13', dpid=NODES[i-1])
            spines.append(spine)

        # Add leaf switches
        for j in range(1, n_leaf + 1):
            leaf = self.addSwitch(f's{j+n_spine}', protocols='OpenFlow13', dpid=NODES[j-1+n_spine])
            leafs.append(leaf)
            for spine in spines:
                self.addLink(leaf, spine, bw=BANDWIDTH, delay=DELAY)
            # add host to all switches
            for k in range(1, n_host + 1):
                host_id = (j-1) * n_host + k
                ip = f"10.0.{j}.{k}/16"
                host = self.addHost(f'h{host_id}', ip=ip)
                self.addLink(host, leaf, port2=k+12, bw=BANDWIDTH, delay=DELAY)

class FatTree(Topo):
    def __init__(self, n_core: int = 1, n_aggr: int = 2, n_edge: int = 2, n_host: int = 4, **opts):
        super(FatTree, self).__init__(**opts)
        core_list = []
        aggr_list = []
        edge_list = []
        node_idx = 0
        # add core switches
        for i in range(n_core):
            core_list.append(self.addSwitch(f"s{i + 1}", protocols='OpenFlow13', dpid=NODES[node_idx]))
            node_idx += 1
        # add aggregation switches
        for j in range(n_aggr):
            aggr = self.addSwitch(f"s{n_core + j + 1}", protocols='OpenFlow13', dpid=NODES[node_idx])
            node_idx += 1
            aggr_list.append(aggr)
            for idx, core in enumerate(core_list):
                self.addLink(aggr, core, port1=2, port2=j + 2, bw=BANDWIDTH, delay=DELAY)
        # add edge switches
        for k in range(n_edge):
            edge = self.addSwitch(f"s{n_core + n_aggr + k + 1}", protocols='OpenFlow13', dpid=NODES[node_idx])
            node_idx += 1
            edge_list.append(edge)
            idx = k % len(aggr_list)
            aggr = aggr_list[idx]
            self.addLink(edge, aggr, port1=2, port2=3, bw=BANDWIDTH, delay=DELAY)
            # add hosts
            for i in range(1, n_host + 1):
                ip = f'10.0.{k + 1}.{i}/16'
                host_id = k * n_host + i
                host = self.addHost(f"h{host_id}", ip=ip)
                self.addLink(host, edge, port2=12 + i, bw=BANDWIDTH, delay=DELAY)

def build_network(n_spine, n_leaf, n_host, topology: str = 'sl', controller_ip: str = "172.17.0.2", controller_port: int = 6653):
    match topology:
        case "sl":
            topo = SpineLeaf()
        case "ft":
            topo = FatTree()
        case "j3c":  # Alias para FatTree
            topo = FatTree()
        case _:
            raise ValueError(f"Topología {topology} no existe. Use 'sl' o 'ft'")
    controller = RemoteController('odl', ip=controller_ip, port=controller_port)
    net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, controller=controller)
    return net

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Define a topology in mininet.")
    parser.add_argument("-t", "--topology", type=str, default='sl',  help="Topology to test (Spine-leaf by default)")
    parser.add_argument("-c", "--controller", type=str, default='172.17.0.2', help="ODL controller ip to use, 172.17.0.2 by default.")
    parser.add_argument("-V", "--verbose", action="store_true", help="Print DPID of the switches created")
    parser.add_argument("--fake_dpid", action="store_false", help="Use false DPID for the switches (False by default)")
    parser.add_argument("--n_spine", type=int, default=2, help="Number of spine switches (Only for spineleal, default 3)")
    parser.add_argument("--n_leaf", type=int, default=3, help="Number of leaf switches (Only for spineleaf, default 2)")
    parser.add_argument("--n_host", type=int, default=3, help="Number of host per leaf/edge switches. (3 by default)")
    args = parser.parse_args()

    match args.topology:
        case "sl":
            topo = SpineLeaf(args.n_spine, args.n_leaf, args.n_host, args.fake_dpid)
        case "ft":
            topo = FatTree()
        case _:
            print(f"Topology {args.topology} don't exists.")
            return 0

    controller = RemoteController('odl', ip=args.controller, port=6653)
    net = Mininet(topo=topo, link=TCLink, switch=OVSSwitch, controller=controller)
    try:
        net.start()
        if args.verbose:
            for switch in net.switches:
                print(f"Switch {switch.name} created with DPID {int(switch.dpid, 16)}")
        CLI(net)
    except KeyboardInterrupt:
        print("\nInterrupted by user\n")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        print("Stopping the network...\n")
        net.stop()

if __name__ == "__main__":
    main()
