# To be moved to the library to share between Columnstore and BuildBot repos


# next ID is #88


CURRENT_BUILD_INFO = "#current-build-info"

DASHBOARD_ICONS = {
    "MariaDB": "database",
    "Connectors": "link",
    "MaxScale": "medium",
}

BUILD_AND_TEST_ES_INTERESTING_STEPS = {
    "creating-source": "#11",
    "building-all": "#12",
    "test-results": "#13"
}

BUILD_AND_TEST_ES_ALL_INTERESTING_STEPS = {
    "build-and-test": "#14",
}

RUN_TEST_INTERESTING_STEPS = {
    "install-test": "#15",
    "one-shot-install-test": "#16",
    "upgrade-test-major": "#17",
    "upgrade-columnstore-test-major": "#86",
    "one-shot-upgrade-test-major": "#01",
    "one-shot-upgrade-test-major-backup": "#39",
    "upgrade-test-minor": "#02",
    "upgrade-columnstore-test-minor": "#87",
    "one-shot-upgrade-test-minor": "#03",
    "one-shot-upgrade-test-minor-backup": "#40",
    "run-maxscale": "#04",
    "test-docker": "#18",
}

BUILD_AND_TEST_IMAGE_INTERESTING_STEPS = {
    "building-packages": "#19",
    "building-tarball": "#20",
    "mtr-tests": "#21",
    "mtr-tests-filesystems": "#41",
    "run-mtr": "#22",
}

BUILD_CONNECTOR_ALL_INTERESTING_STEPS = {
    "building-all": "#23",
    "install-test-all": "#24",
    "install-test": "#25",
}

BUILD_MAXSCALE_ALL_INTERESTING_STEPS = {
    "building-all": "#26",
    "install-test-all": "#27",
    "upgrade-test-all": "#05",
}

RUN_MAXSCALE_TEST_INTERESTING_STEPS = {
    "run-test": "#28",
}

BUILD_GALERA_ALL_INTERESTING_STEPS = {
    "building-all": "#29"
}

BUILD_AND_TEST_MAXSCALE_INTERESTING_STEPS = {
    "build": "#30",
    "run-test": "#31"
}

UPGRADE_ALL_INTERESTING_STEPS = {
    "run-test": "#32"
}

BUILD_AND_TEST_MAXSCALE_PARALL_INTERESTING_STEPS = {
    "build": "#33",
    "run-test": "#34",
    "maxscale-tests": "#35"
}

INSTALL_TEST_ALL_VERSIONS = {
    "install-test-all": "#36",
    "install-test": "#37",
    "one-step-install-test-all": "#06",
    "unsupported-one-step-install-test-all": "#07",
    "full-upgrade-test": "#08",
    "upgrade-test-all": "#09",
    "one-step-upgrade-test-all": "#10",
    "run-test": "#38",
}

INSTALL_TEST_ALL_STEP = {
    "10-4-version": "#42",
    "10-5-version": "#43",
    "10-6-version": "#44",
    "10-9-version": "#45",
    "10-10-version": "#46",
    "10-11-version": "#47",
    "11-0-version": "#48",
    "11-1-version": "#49",
    "11-2-version": "#74",
    "11-3-version": "#78",
    "11-4-version": "#82",
}

INSTALL_TEST_ONE_STEP_ALL_STEP = {
    "10-4-version": "#50",
    "10-5-version": "#51",
    "10-6-version": "#52",
    "10-9-version": "#53",
    "10-10-version": "#54",
    "10-11-version": "#55",
    "11-0-version": "#56",
    "11-1-version": "#57",
    "11-2-version": "#75",
    "11-3-version": "#79",
    "11-4-version": "#83",
}

INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP = {
    "10-4-version": "#58",
    "10-5-version": "#59",
    "10-6-version": "#60",
    "10-9-version": "#61",
    "10-10-version": "#62",
    "10-11-version": "#63",
    "11-0-version": "#64",
    "11-1-version": "#65",
    "11-2-version": "#76",
    "11-3-version": "#80",
    "11-4-version": "#84",
}

UPGRADE_TEST_FULL_STEP = {
    "10-4-version": "#66",
    "10-5-version": "#67",
    "10-6-version": "#68",
    "10-9-version": "#69",
    "10-10-version": "#70",
    "10-11-version": "#71",
    "11-0-version": "#72",
    "11-1-version": "#73",
    "11-2-version": "#77",
    "11-3-version": "#81",
    "11-4-version": "#85",
}
