# Changelog
All notable changes to this project will be documented in this file.

## [0.2.2]
### Fixed
- Fix local cache set existing key, policy should not update

### Changed
- Bump Cacheme-utils version

## [0.2.1]
### Added
- Remove expired nodes automatically
- Add build_node API
- Add missing py.typed

## [0.2.0]
### Added
- Cacheme V2

## [0.1.1]
### Added
- Node support Meta class
- Add stale option to settings/cacheme decorator/node meta
- Node support hit/miss function
- Cacheme tags using nodes instead of cacheme instances

### Removed
- Remove `invalid_all()` method from tag, using `tag.objects.invalid()` instead

## [0.1.0]
### Added
- Add node capability to cacheme.

### Removed
- Get keys from tag, tag only support invalid now.

## [0.0.9]
### Added
- Cacheme first release.
