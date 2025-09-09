## DBRM unresponsive timeout

The master DBRM node previously used a fixed 300s (5 minutes) timeout before forcing read-only mode when a worker didn't respond. This is now configurable via Columnstore.xml only:

- SystemConfig/DBRMUnresponsiveTimeout (seconds, default 300)

The value controls how long the master waits for workers to reconfigure/respond after a network error is detected before switching to read-only. See `versioning/BRM/masterdbrmnode.cpp` for details.

This file documents helpful knowledge, commands and setup instructions for better developing flow.

## Logging

As MariaDB runs as a system service/daemon its logs (and print-outs to stdout) get collected by journalctl (which shows logs from the systemd journal). You can check it with:

```bash
journalctl -u mariadb | tail
```

**Logging different objects**

Rows:

```bash
std::cout << row.toString() << std::endl;
```

## Restarting services after a crash

Sometimes, e.g. during debugging, single processes can crash.

To restart a MCS specific unit process run:

```
systemctl restart mcs-<process_name>
```

(So e.g. `systemctl restart mcs-primproc` ).

If you need to restart the whole installation use

```
systemctl restart mariadb-columnstore
```

## Interaction with MariaDB Server

MCS is one of the many [storage engines](https://mariadb.com/kb/en/choosing-the-right-storage-engine/) offered by MariaDB. A storage engine handles the SQL operations for a certain table type, e.g. Columnstore is designed for big data, uses a columnar storage system and can process petabytes of data. The default engine for MariaDB is InnoDB. You can see all currently available engines on your server by running the SQL statement: `SHOW ENGINES;` in your MariaDB console. Columnstore needs to be explicitly installed, but if you’re reading this you probably built MariaDB and MCS from source for development purposes and should see it listed.

Storage Engines differ in their capabilities. E.g. InnoDB is a “dumb” engine, and doesn’t work itself with the parsed syntax tree and only provides basic (e.g. filtered) output. In contrast, Columnstore is a “smart” engine, which gets the parsed syntax tree and executes the whole query on its own.

For interaction with storage engines, MariaDB has a template that is basically a number of API methods that are used by MariaDB for processing a query. API methods are used by both dumb and smart engines. Handlers are used by smart handlers, e.g. the Select handler is a structure with a couple of methods. The constructor the select handler is another API method. This constructor either returns nullptr or Select handler instance. In the latter case MariaDB hands future processing to the Select Handler instance method via calling its `run`method.

## Troubleshooting

### Killing a process during Debugging

Especially during debugging you might end up killing a process, which leads to error messages like:

`ERROR 1815 (HY000): Internal error: MCS-2033: Error occurred when calling system catalog.`

This error message occurs when the `PrimProc` process is killed, but all other processes continue running and cannot access the system catalog which is served by `PrimProc`.

You can verified that this happened by having a look at all running processes for MariaDB/mysql via:

```bash
ps -axwffu | grep mysql
```

And restart any service via

```bash
systemctl restart mcs-<process_name>
```

### Zombie/Defunct processes

Sometimes Zombie/Defunct processes can cause problems, e.g. when one PrimProc process has become defunct, a new one has been started, but MariaDB still attaches to the defunct process. Refer to [here](https://www.linuxjournal.com/content/how-kill-zombie-processes-linux) for ways to kill Zombie processes. Sometimes a restart of the development machine can be the quickest way to resolve this problem, however.

## Development Setup with Local Linux VM for ARM-based Macs

As MCS only supports Linux-based operating systems, everyone working on a MacOS or Windows system will need some way to access a Linux system. This guide is written and tested for a M1 Mac, but will probably be easily adaptable to other operating systems.

Using the ssh remote development features of common IDEs (e.g. VSCode or CLion), makes developing on a remote Linux server the easiest solution for development from MacOS. However, this has two drawbacks:

1. Development requires a (stable) internet connection and even with sufficient connection can sometimes exhibit weird load times / freezes.
2. Depending on the capabilities of the available remote server, building times might be better on a M1 Mac. (Virtualization, especially with VMWare, works quite nice performance wise.)

Additionally, Vagrant allows to specify provisioning steps, which make setting up a new develop VM as easy as running two commands. (And already have everything necessary installed and built.)

Using the provided Vagrantfile the setup of develop VM is as easy as:

1. Adapting the parameters in the provided Vagrantfile. Currently the following options can/need to be adapted:
    1. `MARIA_DB_SERVER_REPOSITORY` and `MCS_REPOSITORY` . These options expect the HTTPS GitHub URL of the referenced repositories. If a build with a fork of the official repos is wanted, this is where the fork URLs should be provided. (For any questions regarding a general build, please refer to the `BUILD.md`).
    2. `PROVIDER` . Vagrant allows to configure the underlying VM software used (the so called provider). The current version of the Vagrantfile uses VMWare as a VM provider. VMware provides free licenses for personal use, students and open-source development, otherwise it is a paid service. If you don’t have a license or want to use another provider either way, you can either use the out of the box provided VirtualBox provider or install another provider. Read more about Vagrant VM providers [here](https://developer.hashicorp.com/vagrant/docs/providers). Read more about how to install VMWare as a provider [here](https://developer.hashicorp.com/vagrant/docs/providers/vmware/installation).
    3. `BOX` . Vagrant uses boxes to package Vagrant environments. The box needs to match your system and architecture. The easiest way to obtain a a box is to select one from the publicly available, pre-defined boxes at [VagrantCloud](https://app.vagrantup.com/boxes/search).
    4. `MEMSIZE/NUMVCPUS`: Adapt the number of cores and the amount of RAM you want to give your VM.
2. Run `vagrant up` to create and/or start the virtual machine as specified in the `Vagrantfile`.
3. Run `vagrant ssh` to obtain a terminal directly in your VM - or to develop on the virtual machine in your preferred IDE, obtain the ssh config data of the machine with `vagrant ssh-config` and use it to connect. (For even easier connection add the ssh connection data to your `~/.ssh/config` .)