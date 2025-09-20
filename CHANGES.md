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

# Changes in 0.1.1

-   Update fuser for security fixes

# Changes in 0.1.0

Initial release.

