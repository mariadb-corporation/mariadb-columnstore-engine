## Logging

How to log in MCS:

```bash
std::cout << row.toString() << std::endl;
```

Stuff gets printed to journalctl, check with:

```bash
journalctl -u mariadb | tail
```

As mariadb runs as a system service / daemon its logs get collected by journalctl (which shows logs from the systemd journal). 

## Restarting services after a crash

Sometimes e.g. during debugging single processes can crash. 

To restart a mcs-specific unit process run:

```
systemctl restart mcs-<process_name>
```

(So e.g. `systemctl restart mcs-primproc` )

If you need to restart the whole installation use

```
systemctl restart mariadb-columnstore
```

## Interaction with MariaDB Server

MCS is one of the many [storage engines](https://mariadb.com/kb/en/choosing-the-right-storage-engine/) offered by MariaDB. A storage engine handles the SQL operations for a certain table type, e.g. Columnstore is designed for big data, uses a columnar storage system and can process petabytes of data. The default engine for MariaDB is InnoDB. You can see all currently available engines on your server by running the SQL statement: `SHOW ENGINES;` in your mariadb console. Columnstore needs to be explicitly installed, but if you’re reading this you probably built MariaDB and MCS from source for development purposes and should see it listed.

MDB has a notion of a [storage engine](https://mariadb.com/kb/en/choosing-the-right-storage-engine/), e.g. InnoDB, Columnstore. InnoDB is a dumb engine, means the best it can deliver is a filtered output. Columnstore is so-called smart engine. It grabs parsed syntax tree and executes the whole query on its own.

MDB has a template for storage engines that is basically a number of API methods that are used by MDB processing a query. API methods are used by both dumb and smart engines whilst handlers are used by smart handlers, e.g. select handler is a structure with couple methods. SH ctor is another API method. This ctor is either returns nullptr or SH instance. In the latter case MDB hands future processing to SH instance method calling its `run`method.

## Troubleshooting

### Killing a process during Debugging

Especially during debugging you might end up killing a process, which leads to error messages like e.g.:

`ERROR 1815 (HY000): Internal error: MCS-2033: Error occurred when calling system catalog.` 

Which occurs when the `PrimProc` process is killed, but all other processes continue running and then cannot access the system catalog which is served by `PrimProc`.

You can verified that this happened by having a look at all running processes for mariadb/mysql via:

```bash
ps -axwffu | grep mysql
```

And restart any service via 

```bash
systemctl restart mcs-<process_name>
```

How to kill Zombie-Processes:

https://www.linuxjournal.com/content/how-kill-zombie-processes-linux

## Development Setup with Local Linux VM for M1 Macs

As MCS only builds on Linux, everyone working on a MacOS or Windows system will need some access to a Linux system. This guide is written and tested for a M1 Mac, but will probably be easily adaptable to other operating systems.

Using the ssh remote development features of common IDEs (e.g. VSCode or CLion), makes simply developing on a remote Linux server the easiest solution for development from MacOS. However, this has two drawbacks:

1. Development requires a (stable) internet connection and even with sufficient connection can sometimes exhibit weird load times / freezes.
2. Depending on the capabilities of the available remote server, building times might be better on a M1 Mac. (Virtualization especially with VMWare works quite nice performance wise.)

Using the provided Vagrantfile the setup of a VM to develop in is as easy as:

1. Adapting the parameters in the given file. Especially important - the current state of the Vagrantfile uses `vmware` as a VM provider. VMware provides free licenses for personal use, students and open-source development, otherwise it is a paid service. If you don’t have a license or want to use another provider either way, you can either use the out of the box provided `virtualbox` provider or install another provider. Read more about vagrant vm providers here. Read more about how to install vmware as a provider here.
    
    Check if the chosen image works for you (especially regarding the architecture of your machine).
    
    Adapt the number of cores and the amount of RAM you want to give your vm. 
    
2. Run `vagrant up` to create and/or start the virtual machine as specified in the `Vagrantfile`. 
3. Run `vagrant ssh` to obtain a terminal directly in your vm - or to develop on the virtual machine in your preferred IDE, obtain the ssh config data of the machine with `vagrant ssh-config` and use it to connect. (For even easier connection add the ssh connection data to your `~/.ssh/config` .)