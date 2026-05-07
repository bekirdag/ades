# Changelog

## [0.3.2](https://github.com/bekirdag/ades/compare/ades-tool-v0.3.1...ades-tool-v0.3.2) (2026-05-07)


### Bug Fixes

* disable service prewarm on constrained prod startup ([2f7a123](https://github.com/bekirdag/ades/commit/2f7a123200c15442f78344bb608330e7b8fbea37))
* harden production deploy workflow ([1917272](https://github.com/bekirdag/ades/commit/1917272abc05f56deade0ea3d4835d7b76ad72cd))
* reload nginx during prod verification ([9e32f1a](https://github.com/bekirdag/ades/commit/9e32f1ae9854315febd1c667d7983128dc0e345c))
* repair deploy install script quoting ([b2c0f7e](https://github.com/bekirdag/ades/commit/b2c0f7ebacb0f20563321889f8bfe77d59a80258))
* retry flaky production ssh deploys ([da6244a](https://github.com/bekirdag/ades/commit/da6244ada5efc294d50b26a10eaccb5097babc4c))

## [0.3.1](https://github.com/bekirdag/ades/compare/ades-tool-v0.3.0...ades-tool-v0.3.1) (2026-04-22)


### Bug Fixes

* restore deploy-safe matcher provenance and validation ([c99426d](https://github.com/bekirdag/ades/commit/c99426d768c5ab7a89ebcc2b031a4b2df027a8b1))

## [0.3.0](https://github.com/bekirdag/ades/compare/ades-tool-v0.2.6...ades-tool-v0.3.0) (2026-04-22)


### Features

* expand finance country packs and release support ([028a739](https://github.com/bekirdag/ades/commit/028a739489f684deb68caaf5a0ff9fb427ba4af7))


### Bug Fixes

* fall back to graph-derived hybrid seed vectors ([6c45fc4](https://github.com/bekirdag/ades/commit/6c45fc4abe039d2943eea4dde34fee6efdbb66b7))
* restore graph-context vector refinement ([07c1f19](https://github.com/bekirdag/ades/commit/07c1f199ee41ec2a7f1a16091ba726c621881c34))
* suppress low-value contextual org acronyms ([85940a5](https://github.com/bekirdag/ades/commit/85940a52e93c34f107371d856fa86faefd048127))
* wire hybrid vector proposals on release line ([f244ada](https://github.com/bekirdag/ades/commit/f244adaa8bfca69fb51efa1394a4b85e5281a03d))

## [0.2.6](https://github.com/bekirdag/ades/compare/ades-tool-v0.2.5...ades-tool-v0.2.6) (2026-04-21)


### Bug Fixes

* publish npm via oidc in unified release workflow ([e0c6419](https://github.com/bekirdag/ades/commit/e0c64197dc238f971e0afd0145f08bde606bd7bf))

## [0.2.5](https://github.com/bekirdag/ades/compare/ades-tool-v0.2.4...ades-tool-v0.2.5) (2026-04-21)


### Bug Fixes

* publish latest release packages ([346b42b](https://github.com/bekirdag/ades/commit/346b42b40a71d7e7a31a87d61a5aece506dcfd5b))

## [0.2.4](https://github.com/bekirdag/ades/compare/ades-tool-v0.2.3...ades-tool-v0.2.4) (2026-04-21)


### Bug Fixes

* rebuild matchers from metadata store ([b247eb3](https://github.com/bekirdag/ades/commit/b247eb32518aa3d502e17eff4bf78d3f5752aaae))

## [0.2.3](https://github.com/bekirdag/ades/compare/ades-tool-v0.2.2...ades-tool-v0.2.3) (2026-04-20)


### Bug Fixes

* publish npm from dedicated workflow ([ba57337](https://github.com/bekirdag/ades/commit/ba57337a0031a4466aa851b67e24eb640035e566))

## [0.2.2](https://github.com/bekirdag/ades/compare/ades-tool-v0.2.1...ades-tool-v0.2.2) (2026-04-20)


### Bug Fixes

* make release validation artifact staging idempotent ([e58eab7](https://github.com/bekirdag/ades/commit/e58eab77d4f8b78abe61eee9e0fd9ac4b2d813c5))

## [0.2.1](https://github.com/bekirdag/ades/compare/ades-tool-v0.2.0...ades-tool-v0.2.1) (2026-04-20)


### Bug Fixes

* repair release validation after package rename ([0ec94f1](https://github.com/bekirdag/ades/commit/0ec94f1e210122552561a637f29f57f66db81a9f))
* update deploy workflow wheel glob ([f7bb6f0](https://github.com/bekirdag/ades/commit/f7bb6f0abeb847d3fc1ba788636a0a05cb962e68))

## [0.2.0](https://github.com/bekirdag/ades/compare/ades-tool-v0.1.0...ades-tool-v0.2.0) (2026-04-20)


### Features

* add graph-backed pack tooling and automated releases ([98c2b9d](https://github.com/bekirdag/ades/commit/98c2b9dc79b7a31ce979f45cfa5a92cf04c0af6f))
* add hosted vector qid graph refinement ([9def043](https://github.com/bekirdag/ades/commit/9def0430be14570bfc43432d246f27041dd37157))


### Bug Fixes

* restore green main for production deploy ([0558592](https://github.com/bekirdag/ades/commit/0558592f5a03bbe47a6440db976630ea931dbce0))
* restore pack runtime cache helper ([b290b2d](https://github.com/bekirdag/ades/commit/b290b2d6a013f4dfc71bb86c9a59413277a86b03))
* stabilize colored cli help assertions ([edcd246](https://github.com/bekirdag/ades/commit/edcd246d3eccdf892cc47b88572fbf82e1aeb117))
