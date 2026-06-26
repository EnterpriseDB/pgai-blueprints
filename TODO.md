# Issues

There are 7 issues found by InfoSec:

 1. Finding 1 - DONE - CRIT - (bind to 127.0.0.1)

    [GC, MG, RR] This is "Unauthenticated command execution via the
    chat agent" composed of three separate facts:

      - (a) Agent binds to 0.0.0.0
      - (b) API endpoints are not authenticated (Finding 2)
      - (c) run_command runs arbitrary shell (Finding 5)

 2. Finding 2 - DONE - CRIT - (unauthenticated API)

    [GC] Solution merged to the main branch: a patch made with Claude,
    and amended with Claude too, which now seems to work.

    It introduces a `AGENT_API_KEY` (`agent123`) which the user must
    enter in the website before connecting to the Blueprints main
    page.

 3. Finding 3 - DONE - HIGH - (SECURITY.md untrue)

    [RS] Solution merged to the main branch.

 4. Finding 4 - DONE - HIGH - (network-exposed dbs)

    [GC] Solution merged to the main branch.  We now bind ports only
    to individual IPs no longer using the blanket 0.0.0.0 range.

 5. Finding 5 - DONE - MED  - (no allowlist in run_command)

    [AG] Solution merged to the main branch.

 6. Finding 6 - DONE - MED  - (empty .gitignore)

    [GC] Solution merged to the public repo only, main branch.

    Not relevant as the git repo is used only as a way to publish
    code, to which we do not commit outside of code releases.

    At the same time, the fix is very easy as the report suggests what
    lines to add.

 7. Finding 7 - TODO - LOW - (lower priority findings)

Then, there is some feedback about the user facing docs:

 8. Partner language - TODO - MED

    In the [Technology Stack](README.md#technology-stack) section, we
    use the term "Partner" even if there is no formal partnership with
    those organizations. Perhaps we should call them "Components"?

 9. Disclaimer - TODO - MED

    In the [README](README.md) we call EDB Postgres® AI Blueprints a
    "ready-to-deploy reference architecture", while in the
    [SECURITY](SECURITY.md) file we place a disclaimer:

    > **This project is designed for local development and
    > proof-of-concept demonstrations only. It is NOT intended for
    > production use without additional security hardening.**

    The disclaimer is clearer, but it is restricted to security. I
    would also mention High Availability too, moving the disclaimer in
    the toplevel README. For instance:

    > **This project is intended for proof-of-concept demonstrations
    > only; it is NOT intended for production use, which requires
    > additional security hardening, and appropriate modifications to
    > the architecture that enable high availability procedures.**

# User Facing Documentation

There are 5 documentation files that are currently marked for
distribution:

  - [Toplevel README](README.md)
  - [Toplevel SECURITY](SECURITY.md)
  - [`pg-airman-mcp` plugin README](plugins/pg-airman-mcp/README.md)
  - [BFSI stack README](stacks/bfsi-fraud-detection/README.md)
  - [BFSI stack ARCHITECTURE](stacks/bfsi-fraud-detection/ARCHITECTURE.md)
