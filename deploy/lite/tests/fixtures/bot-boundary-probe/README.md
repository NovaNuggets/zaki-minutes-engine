# Bot boundary probe

`index.js` is mounted into the disposable permission-test container and run
through the same trusted per-meeting launcher used by Lite. It fails unless the
workload has a unique non-root UID, an empty capability set, `no_new_privs`, an
isolated home, access only to the shared audio socket, and no access to
operator credentials or a sibling meeting process environment.

The probe is test-only and contains no production service logic or secrets.
