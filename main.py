"""
MosyleSnipeSync main script.
Synchronizes Apple device data from Mosyle to Snipe-IT.
Can run as a one-time sync or as a scheduled daemon.
"""
import json
import datetime
import configparser
import argparse
import time
import sys
import os
from pathlib import Path
from rich.progress import Progress
from rich.console import Console

from mosyle import Mosyle
from snipe import Snipe
from logger_config import setup_logging, get_logger


def load_configuration(config_file='settings.ini'):
    """Load configuration from settings.ini."""
    logger = get_logger()

    # Load configuration file
    if not Path(config_file).exists():
        logger.error(f"Configuration file not found: {config_file}")
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    config = configparser.ConfigParser(interpolation=None)
    config.read(config_file)

    # Extract Mosyle config
    try:
        mosyle_url = config['mosyle']['url']
        mosyle_token = config['mosyle']['token']
        mosyle_user = config['mosyle']['user']
        mosyle_password = config['mosyle']['password']
        deviceTypes = config['mosyle']['deviceTypes'].split(',')
        calltype = config['mosyle'].get('calltype', 'all')
    except KeyError as e:
        logger.error(f"Missing required Mosyle configuration: {e}")
        raise ValueError(f"Missing required Mosyle configuration: {e}")

    # Verify required Mosyle credentials
    if not all([mosyle_url, mosyle_token, mosyle_user, mosyle_password]):
        logger.error("Missing required Mosyle credentials in settings.ini")
        raise ValueError("Missing Mosyle credentials in settings.ini [mosyle] section")

    # Extract Snipe-IT config
    try:
        snipe_url = config['snipe-it']['url']
        apiKey = config['snipe-it']['apiKey']
        apple_manufacturer_id = config['snipe-it']['manufacturer_id']
        macos_category_id = config['snipe-it']['macos_category_id']
        ios_category_id = config['snipe-it']['ios_category_id']
        tvos_category_id = config['snipe-it']['tvos_category_id']
        macos_fieldset_id = config['snipe-it']['macos_fieldset_id']
        ios_fieldset_id = config['snipe-it']['ios_fieldset_id']
        tvos_fieldset_id = config['snipe-it']['tvos_fieldset_id']
        snipe_rate_limit = int(config['snipe-it']['rate_limit'])
        apple_image_check = config['snipe-it'].getboolean('apple_image_check')
    except KeyError as e:
        logger.error(f"Missing required configuration key: {e}")
        raise

    logger.info("Configuration loaded successfully")

    return {
        'mosyle': {
            'url': mosyle_url,
            'token': mosyle_token,
            'user': mosyle_user,
            'password': mosyle_password,
            'deviceTypes': deviceTypes,
            'calltype': calltype
        },
        'snipe': {
            'url': snipe_url,
            'apiKey': apiKey,
            'manufacturer_id': apple_manufacturer_id,
            'macos_category_id': macos_category_id,
            'ios_category_id': ios_category_id,
            'tvos_category_id': tvos_category_id,
            'macos_fieldset_id': macos_fieldset_id,
            'ios_fieldset_id': ios_fieldset_id,
            'tvos_fieldset_id': tvos_fieldset_id,
            'rate_limit': snipe_rate_limit,
            'apple_image_check': apple_image_check
        }
    }


def run_sync(config):
    """
    Execute a single synchronization run.

    Args:
        config: Configuration dictionary from load_configuration()

    Returns:
        int: Total number of devices processed
    """
    logger = get_logger()
    console = Console()

    logger.info("=== Starting synchronization run ===")

    try:
        # Initialize Mosyle
        mosyle = Mosyle(
            config['mosyle']['token'],
            config['mosyle']['user'],
            config['mosyle']['password'],
            config['mosyle']['url']
        )
        logger.info("Successfully connected to Mosyle")
    except Exception as e:
        logger.error(f"Failed to connect to Mosyle: {e}")
        raise

    try:
        # Initialize Snipe-IT
        snipe = Snipe(
            config['snipe']['apiKey'],
            config['snipe']['url'],
            config['snipe']['manufacturer_id'],
            config['snipe']['macos_category_id'],
            config['snipe']['ios_category_id'],
            config['snipe']['tvos_category_id'],
            config['snipe']['rate_limit'],
            config['snipe']['macos_fieldset_id'],
            config['snipe']['ios_fieldset_id'],
            config['snipe']['tvos_fieldset_id'],
            config['snipe']['apple_image_check']
        )
        logger.info("Successfully connected to Snipe-IT")
    except Exception as e:
        logger.error(f"Failed to connect to Snipe-IT: {e}")
        raise

    total_devices_processed = 0
    ts = datetime.datetime.now().timestamp() - 200

    for deviceType in config['mosyle']['deviceTypes']:
        deviceType = deviceType.strip()
        logger.info(f"Processing device type: {deviceType}")

        try:
            # Fetch devices from Mosyle
            if config['mosyle']['calltype'] == "timestamp":
                logger.debug(f"Using timestamp mode for {deviceType}")
                mosyle_response = mosyle.listTimestamp(ts, ts, deviceType)
            else:
                logger.debug(f"Using 'all' mode for {deviceType} (paginated)")
                all_devices = []
                page = 1
                while True:
                    response = mosyle.list(deviceType, page=page)
                    devices = response.get('response', {}).get('devices', [])
                    if not devices:
                        break
                    all_devices.extend(devices)
                    logger.debug(f"Retrieved {len(devices)} devices from page {page}")
                    page += 1
                mosyle_response = {"status": "OK", "response": {"devices": all_devices}}

            if mosyle_response.get('status') != "OK":
                logger.error(f"Mosyle API error for {deviceType}: {mosyle_response.get('message')}")
                continue

            devices = mosyle_response['response'].get('devices', [])
            device_count = len(devices)
            logger.info(f"Found {device_count} {deviceType} devices in Mosyle")

            # Process each device
            with Progress() as progress:
                task = progress.add_task(f"[green]Processing {deviceType} devices...", total=device_count)

                for device_index, sn in enumerate(devices, 1):
                    try:
                        if sn['serial_number'] is None:
                            logger.warning(f"{deviceType} device at index {device_index} has no serial number, skipping")
                            progress.advance(task)
                            continue

                        # Look up existing asset
                        asset_response = snipe.listHardware(sn['serial_number'])
                        if asset_response is None:
                            logger.error(f"Failed to search asset {sn['serial_number']}: API request failed")
                            progress.advance(task)
                            continue
                        asset = asset_response.json()

                        # Look up or create model
                        model_response = snipe.searchModel(sn['device_model'])
                        if model_response is None:
                            logger.error(f"Failed to search model for {sn['device_model']}: API request failed")
                            progress.advance(task)
                            continue

                        model = model_response.json()
                        if model['total'] == 0:
                            logger.info(f"Creating new model: {sn['device_model']}")
                            if sn['os'] == "mac":
                                create_response = snipe.createModel(sn['device_model'])
                            elif sn['os'] == "ios":
                                create_response = snipe.createMobileModel(sn['device_model'])
                            elif sn['os'] == "tvos":
                                create_response = snipe.createAppleTvModel(sn['device_model'])
                            else:
                                logger.error(f"Unknown OS type: {sn['os']}")
                                progress.advance(task)
                                continue

                            if create_response is None:
                                logger.error(f"Failed to create model for {sn['device_model']}: API request failed")
                                progress.advance(task)
                                continue

                            model = create_response.json()['payload']['id']
                        else:
                            model = model['rows'][0]['id']

                        # Check for assigned user
                        mosyle_user = sn.get('useremail') if sn.get('CurrentConsoleManagedUser') and 'useremail' in sn else None
                        devicePayload = snipe.buildPayloadFromMosyle(sn)

                        # Create asset if doesn't exist
                        if asset.get('total', 0) == 0:
                            logger.info(f"Creating new asset: {sn['serial_number']} ({sn['device_model']})")
                            create_asset_response = snipe.createAsset(model, devicePayload)
                            if create_asset_response is None:
                                logger.error(f"Failed to create asset for {sn['serial_number']}: API request failed")
                                progress.advance(task)
                                continue
                            asset = create_asset_response
                            if mosyle_user:
                                logger.info(f"Assigning asset to user: {mosyle_user}")
                                snipe.assignAsset(mosyle_user, asset['payload']['id'])
                            total_devices_processed += 1
                            progress.advance(task)
                            continue

                        # Update existing asset
                        if asset.get('total') == 1 and asset.get('rows'):
                            logger.info(f"Updating asset: {sn['serial_number']}")
                            snipe.updateAsset(asset['rows'][0]['id'], devicePayload, model)

                        # Sync user assignment
                        if mosyle_user:
                            assigned = asset['rows'][0]['assigned_to']
                            if assigned is None and sn.get('useremail'):
                                logger.info(f"Assigning asset to user: {sn['useremail']}")
                                snipe.assignAsset(sn['useremail'], asset['rows'][0]['id'])
                            elif sn.get('useremail') is None:
                                logger.info(f"Unassigning asset: {asset['rows'][0]['id']}")
                                snipe.unasigneAsset(asset['rows'][0]['id'])
                            elif assigned and assigned['username'] != sn['useremail']:
                                logger.info(f"Reassigning asset from {assigned['username']} to {sn['useremail']}")
                                snipe.unasigneAsset(asset['rows'][0]['id'])
                                snipe.assignAsset(sn['useremail'], asset['rows'][0]['id'])

                        # Sync asset tag back to Mosyle
                        asset_tag = asset['rows'][0].get('asset_tag') if asset.get('rows') else None
                        if not sn.get('asset_tag') or sn['asset_tag'] != asset_tag:
                            if asset_tag:
                                logger.info(f"Syncing asset tag to Mosyle: {sn['serial_number']} -> {asset_tag}")
                                mosyle.setAssetTag(sn['serial_number'], asset_tag)

                        total_devices_processed += 1
                        progress.advance(task)

                    except Exception as e:
                        logger.error(f"Error processing device {sn.get('serial_number', 'unknown')}: {e}")
                        progress.advance(task)
                        continue

            logger.info(f"Finished {deviceType}: {total_devices_processed} total devices processed")

        except Exception as e:
            logger.error(f"Error processing device type {deviceType}: {e}")
            continue

    logger.info(f"=== Synchronization run complete. Total devices processed: {total_devices_processed} ===")
    return total_devices_processed


def main():
    """Main entry point supporting both one-time and daemon modes."""
    parser = argparse.ArgumentParser(
        description='Synchronize Apple devices from Mosyle to Snipe-IT'
    )
    parser.add_argument(
        '--daemon',
        action='store_true',
        help='Run in daemon mode (continuously loop)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=3600,
        help='Interval between runs in seconds (default: 3600 = 1 hour). Only used in daemon mode.'
    )
    parser.add_argument(
        '--config',
        default='settings.ini',
        help='Path to settings.ini file (default: settings.ini)'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (default: INFO)'
    )
    parser.add_argument(
        '--log-dir',
        default='logs',
        help='Directory for log files (default: logs)'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_dir=args.log_dir, log_level=args.log_level)
    logger = get_logger()

    logger.info("MosyleSnipeSync started")
    logger.info(f"Mode: {'daemon' if args.daemon else 'one-time'}")
    if args.daemon:
        logger.info(f"Interval: {args.interval} seconds ({args.interval / 3600:.1f} hours)")

    try:
        # Load configuration
        config = load_configuration(args.config)

        if args.daemon:
            # Daemon mode: run continuously
            logger.info("Entering daemon mode")
            run_count = 0
            while True:
                try:
                    run_count += 1
                    logger.info(f"--- Run {run_count} ---")
                    run_sync(config)
                    logger.info(f"Sleeping for {args.interval} seconds")
                    time.sleep(args.interval)
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, exiting daemon mode")
                    break
                except Exception as e:
                    logger.error(f"Error in daemon run {run_count}: {e}")
                    logger.info(f"Sleeping for {args.interval} seconds before retry")
                    time.sleep(args.interval)
        else:
            # One-time mode: run once and exit
            run_sync(config)
            logger.info("Exiting")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
