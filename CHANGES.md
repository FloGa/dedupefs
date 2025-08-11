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

# Changes in 0.1.0

Initial release.
