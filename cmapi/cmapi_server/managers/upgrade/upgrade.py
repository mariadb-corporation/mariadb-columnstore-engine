    def updowngrade(self):
        check_gtid_strict_mode()
        # check_mariadb_versions

        # Stop All
        init_cs_down
        wait_cs_down
        stop_mariadb
        stop_cmapi

        # Make backups of configurations, dbrms
        pre_upgrade_dbrm_backup
        pre_upgrade_configuration_backup

        # Upgrade
        do_enterprise_upgrade

        # Start All
        printf "\nStartup\n"
        start_mariadb
        start_cmapi
        init_cs_up

        # Post Upgrade
        confirm_dbrmctl_ok
        run_mariadb_upgrade