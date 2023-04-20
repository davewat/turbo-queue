# Changelog

Notable changes to this project will be documented in this file.  Breaking changes until 1.0

## [Unreleased]
- improve logging
- improve documentation
- examples
- process specific connectors

## [0.7.2] - 2023-04-20

### Fixed
- Update class names in Kafka connector

## [0.7.1] - 2023-04-20

### Added

- Minor documentation updates

### Changed

- Fix class names to PEP standards - will break existing references

## [0.7.0] - 2023-04-19

### Added

- KafkaEnqueue, KafkaDequeue - Kafka connectors for Consumer, Producer
- MultiTask - Scaffolding for building large multi-processing application, useful for scaling Turbo Queue
- Update documentation to use Semantic Versioning

### Changed

- New file name pattern (load,ready,unload).

## [0.5.0] - 2023-0

- Timer function to clear out slow batches.
- Cleanup class to clean-up leftover events from restart.  Ensures events still on disk are reloaded into the queue to be sent.
- Standardized naming convention of the queue files.
- Troubleshooting documentation started.

# Notes

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).