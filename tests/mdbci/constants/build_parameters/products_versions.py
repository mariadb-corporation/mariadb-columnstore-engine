# To be moved to the library to share between Columnstore and BuildBot repos


from constants import dashboard

# MARIADB_STABLE_VERSIONS and MDBE_STABLE_VERSIONS keys have to be the same
# TODO: can it be configurable externally?
MARIADB_RELEASED_VERSIONS = {
   '10.2': {  # EOL
        'oldVersion': '10.2',
        'newVersion': '10.2.44',
        'targetVersion': '10.2.44',
    },
    '10.3': {  # EOL
        'oldVersion': '10.3',
        'newVersion': '10.3.39',
        'targetVersion': '10.3.39',
    },
    '10.4': {
        'oldVersion': '10.4',
        'newVersion': '10.4.33',
        'targetVersion': '10.4.33',
    },
    '10.5': {
        'oldVersion': '10.5',
        'newVersion': '10.5.24',
        'targetVersion': '10.5.24',
    },
    '10.6': {
        'oldVersion': '10.6',
        'newVersion': '10.6.17',
        'targetVersion': '10.6.17',
    },
    '10.7': {  # EOL
        'oldVersion': '10.7',
        'newVersion': '10.7.8',
        'targetVersion': '10.7.8',
    },
    '10.8': {  # EOL
        'oldVersion': '10.8',
        'newVersion': '10.8.7',
        'targetVersion': '10.8.7',
    },
    '10.9': { # EOL
        'oldVersion': '10.9',
        'newVersion': '10.9.8',
        'targetVersion': '10.9.8',
    },
    '10.10': { # EOL
        'oldVersion': '10.10',
        'newVersion': '10.10.7',
        'targetVersion': '10.10.7',
    },
    '10.11': {
        'oldVersion': '10.10',
        'newVersion': '10.11.7',
        'targetVersion': '10.11.7',
    },
    '11.0': {
        'oldVersion': '11.0',
        'newVersion': '11.0.5',
        'targetVersion': '11.0.5',
    },
    '11.1': {
        'oldVersion': '11.1',
        'newVersion': '11.1.4',
        'targetVersion': '11.1.4',
    },
    '11.2': {
        'oldVersion': '11.2',
        'newVersion': '11.2.3',
        'targetVersion': '11.2.3',
    },
    '11.3': {
        'oldVersion': '11.3.1',
        'newVersion': '11.3.2',
        'targetVersion': '11.3.2',
    },
    '11.4': {
        'oldVersion': '11.3',
        'newVersion': '11.4.1',
        'targetVersion': '11.4.1',
    },
}

MDBE_RELEASED_VERSIONS = {
    '10.2': {
        'oldVersion': '10.2',
        'newVersion': '10.2.44-16',
        'targetVersion': '10.2.44',
    },
    '10.3': {
        'oldVersion': '10.3',
        'newVersion': '10.3.39-20',
        'targetVersion': '10.3.39',
    },
    '10.4': {
        'oldVersion': '10.4',
        'newVersion': '10.4.32-22',
        'targetVersion': '10.4.32',
    },
    '10.5': {
        'oldVersion': '10.5',
        'newVersion': '10.5.23-17',
        'targetVersion': '10.5.23',
    },
    '10.6': {
        'oldVersion': '10.6',
        'newVersion': '10.6.16-11',
        'targetVersion': '10.6.16',
    },
    '23.07': {
        'oldVersion': '10.6',
        'newVersion': '23.07.0',
        'targetVersion': '23.07.0',
    },
    '23.08': {
        'oldVersion': '10.6',
        'newVersion': '23.08.0',
        'targetVersion': '23.08.0',
    },
}

MARIADB_CURRENT_VERSIONS = {
    '10.4': {
        'oldVersion': '10.4',
        'newVersion': '10.4.34',
        'targetVersion': '10.4.34',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['10-4-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['10-4-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['10-4-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['10-4-version'],
    },
    '10.5': {
        'oldVersion': '10.5',
        'newVersion': '10.5.25',
        'targetVersion': '10.5.25',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['10-5-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['10-5-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['10-5-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['10-5-version'],
    },
    '10.6': {
        'oldVersion': '10.6',
        'newVersion': '10.6.18',
        'targetVersion': '10.6.18',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['10-6-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['10-6-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['10-6-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['10-6-version'],
    },
    '10.11': {
        'oldVersion': '10.11',
        'newVersion': '10.11.8',
        'targetVersion': '10.11.8',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['10-11-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['10-11-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['10-11-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['10-11-version'],
    },
    '11.0': {
        'oldVersion': '11.0',
        'newVersion': '11.0.6',
        'targetVersion': '11.0.6',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['11-0-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['11-0-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['11-0-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['11-0-version'],
    },
    '11.1': {
        'oldVersion': '11.1',
        'newVersion': '11.1.5',
        'targetVersion': '11.1.5',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['11-1-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['11-1-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['11-1-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['11-1-version'],
    },
    '11.2': {
        'oldVersion': '11.1',
        'newVersion': '11.2.4',
        'targetVersion': '11.2.4',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['11-2-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['11-2-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['11-2-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['11-2-version'],
    },
    '11.3': {
        'oldVersion': '11.3.2',
        'newVersion': '11.3.3',
        'targetVersion': '11.3.3',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['11-3-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['11-3-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['11-3-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['11-3-version'],
    },
    '11.4': {
        'oldVersion': '11.4.1',
        'newVersion': '11.4.2',
        'targetVersion': '11.4.2',
        'installTestTag': dashboard.INSTALL_TEST_ALL_STEP['11-4-version'],
        'installTestOneStepTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_STEP['11-4-version'],
        'installTestOneStepUnsupportedTag': dashboard.INSTALL_TEST_ONE_STEP_ALL_UNSUPPORTED_STEP['11-4-version'],
        'upgradeTestFullTag': dashboard.UPGRADE_TEST_FULL_STEP['11-4-version'],
    },
}

MDBE_CURRENT_VERSIONS = {
    '10.3': {
        'oldVersion': '10.3',
        'newVersion': '10.3.39-20',
        'targetVersion': '10.3.39',
    },
    '10.4': {
        'oldVersion': '10.4',
        'newVersion': '10.4.33-23',
        'targetVersion': '10.4.33',
    },
    '10.5': {
        'oldVersion': '10.5',
        'newVersion': '10.5.24-18',
        'targetVersion': '10.5.24',
    },
    '10.6': {
        'oldVersion': '10.6',
        'newVersion': '10.6.17-12',
        'targetVersion': '10.6.17',
    },
    '23.07': {
        'oldVersion': '10.6',
        'newVersion': '23.07.1',
        'targetVersion': '23.07.1',
    },
    '23.08': {
        'oldVersion': '10.6',
        'newVersion': '23.08.1',
        'targetVersion': '23.08.1',
    },
}


SERVER_VERSIONS_SETS = {
    'MARIADB_CURRENT_VERSIONS': MARIADB_CURRENT_VERSIONS,
    'MARIADB_RELEASED_VERSIONS': MARIADB_RELEASED_VERSIONS,
    'MDBE_CURRENT_VERSIONS': MDBE_CURRENT_VERSIONS,
    'MDBE_RELEASED_VERSIONS': MDBE_RELEASED_VERSIONS,
}

MAXSCALE_RELEASED_VERSIONS = {
    '2.5': {
        'version': '2.5.29',
        'distros': 'ALL',
    },
    '6': {
        'version': '6.4.15',
        'distros': 'ALL',
    },
    '22.08': {
        'version': '22.08.12',
        'distros': 'ALL',
    },
    '23.02': {
        'version': '23.02.9',
        'distros': 'ALL',
    },
    '23.08': {
        'version': '23.08.5',
        'distros': 'ALL',
    },
    '24.02': {
        'version': '24.02.0',
        'distros': 'ALL',
    },
}

LAST_MAXSCALE_MAJOR = list(MAXSCALE_RELEASED_VERSIONS.keys())[-1]
