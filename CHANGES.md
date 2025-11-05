# Changes since latest release

-   Use MetadataExt from unix instead of linux

    This allows us to support MacOS, which can use the unix packages.

-   Add Cross configuration

-   Enable cross builds for supported Linux targets

-   Gracefully continue if CtrlC handler cannot be set

    This happens mainly in tests when multiple CtrlC handlers are being set
    up, but even in real life, failure to set the handler should not
    terminate the execution, you can always manually unmount.

-   Do not write the cache if it is unchanged

-   Make readdir more robust

    By collecting all relevant entries in opendir and using this Vector in
    readdir, a possible changing directory will not jeopardize the execution
    anymore.

-   Re-factor open and read methods

-   Add proper error handling in read

-   Better tracking and reuse of handle ids

-   Make declutter_level configurable

-   Upgrade crazy-deduper

    This also introduces a new version of the cache file format.

    Besides many internal fixes and optimizations, the new cache format
    brings:

        -   Short keys: No more "nanos_since_epoch" all over the place,
            instead we use one-letter-keys now.
        -   Hashing algorithm is only stored once now.
        -   Paths are stored in a nested fashion.

        This results in a much smaller cache file than before, even with
        bigger dedupe caches.

        Furthermore, the cache format version is implemented in a way that it
        will always be backward compatible. Older cache formats will always be
        valid for reading. For writing, the latest cache format will be used.

# Changes in 0.1.1

-   Update fuser for security fixes

# Changes in 0.1.0

Initial release.
