# -*- mode: ruby -*-
# vi: set ft=ruby :

# This Vagrantfile is used to create a local VM for MariaDB ColumnStore development.
# If not disabled, it automatically installs the latest develop branch of MariaDB ColumnStore on the VM.
# Basic usage:
#   1. Install Vagrant and optionally VMWare as a provider.
#   2. Configure the options below to your needs.
#   3. Run "vagrant up" to create the VM and "vagrant ssh" to access it.
# More documentation can be found in DEVELOPING.md.

VAGRANTFILE_API_VERSION = "2"

# Update with your own fork of MariaDB Server and MariaDB ColumnStore if needed.
MARIA_DB_SERVER_REPOSITORY = "https://github.com/MariaDB/server.git"
MCS_REPOSITORY = "https://github.com/mariadb-corporation/mariadb-columnstore-engine.git"

# Set provider for the VM.
# PROVIDER = :virtualbox # default - if you don't have installed a specific provider use this one.
PROVIDER = :vmware_fusion

# Choose image/"box" for the VM for your architecture from https://app.vagrantup.com/boxes/search
BOX = "bento/ubuntu-20.04-arm64"

# Set hardware resources for the VM
MEMSIZE = 16384 #MB
NUMVCPUS = 8

$provision_script = <<-'SCRIPT'
# enable autocompletion via arrow up and down keys using readline library
cat > ~/.inputrc <<-'AUTOCOMPLETION'
  # Respect default shortcuts.
  $include /etc/inputrc
  ## arrow up
  "\e[A":history-search-backward
  ## arrow down
  "\e[B":history-search-forward
AUTOCOMPLETION

# run setup for MariaDB ColumnStore

git clone $1
cd server
git submodule update --init --recursive --depth=1

cd storage/columnstore/columnstore
git remote remove origin
git remote add origin $2

cd server/storage/columnstore/columnstore
git checkout develop
git config --global --add safe.directory `pwd`

sudo -E build/bootstrap_mcs.sh --install-deps -t RelWithDebInfo
SCRIPT

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.define "mcs-dev"
  config.vm.box = BOX
  
  config.vm.provider PROVIDER do |v|
    v.vmx["memsize"] = MEMSIZE
    v.vmx["numvcpus"] = NUMVCPUS
  end

  config.vm.provision "shell" do |script|
    script.inline = $provision_script
    script.args = [MARIA_DB_SERVER_REPOSITORY, MCS_REPOSITORY]
    script.privileged = false
  end
end
