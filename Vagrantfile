# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "bento/ubuntu-20.04-arm64"
  
  config.vm.provider :vmware_fusion do |v|
    v.vmx["memsize"] = "16384"
    v.vmx["numvcpus"] = "8"
  end

#   config.vm.provision :shell, :inline => <<-SCRIPT
#     cat > ~/.inputrc <<-EOF
#       # Respect default shortcuts.
#       $include /etc/inputrc
#       ## arrow up
#       "\e[A":history-search-backward
#       ## arrow down
#       "\e[B":history-search-forward
#     EOF
#     bind -f ~/.inputrc

#     git clone https://github.com/MariaDB/server.git
#     cd server
#     git submodule update --init --recursive --depth=1

#     cd storage/columnstore/columnstore
#     git remote -v #this should return: origin https://github.com/mariadb-corporation/mariadb-columnstore-engine.git (fetch)
#     git remote remove origin
#     git remote add origin https://github.com/phoeinx/mariadb-columnstore-engine

#     cd server/storage/columnstore/columnstore
#     git checkout develop
#     git config --global --add safe.directory `pwd`

#     sudo -E build/bootstrap_mcs.sh --install-deps -t RelWithDebInfo
#   SCRIPT
end
