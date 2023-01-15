#!/usr/bin/env python3

import logging

# setup logging

def configure_bbsn00():
    CONFIG = {}
    CONFIG['data_root'] = '/hot/data/'
    return CONFIG

def configure(preset_name):
    if 'bbsn00' == preset_name:
        return configure_bbsn00()
    else:
        raise Exception(f'unknown preset {preset_name}')

def main():
    pass


if __name__ == '__main__':
    main()